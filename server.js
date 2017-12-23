#!/bin/env node

var mongo = require('mongodb');
var ObjectId = require('mongodb').ObjectId;
var WebSocketServer = require("ws").Server
var http = require("http")
var express = require("express")
var app = express()

var ipaddress = process.env.IP || process.env.OPENSHIFT_NODEJS_IP || '0.0.0.0';
var port = process.env.PORT || process.env.OPENSHIFT_NODEJS_PORT || 8080;
var isOpenShift = process.env.OPENSHIFT_NODEJS_PORT ? true : false;

app.use(express.static(__dirname + "/"))

console.log("IP: " + ipaddress + " Port: " + port
    + (isOpenShift ? " OpenShift mode" : " Standalone mode"));

var server = http.createServer(app)
server.listen(port, ipaddress)

console.log("http server listening on %s:%d", ipaddress, port)

var wss = new WebSocketServer({ server: server })
console.log("websocket server created")

var MongoClient = require('mongodb').MongoClient;

// default to a 'localhost' configuration:
var DBurl = "mongodb://localhost:27017/PDSdb";
// if OPENSHIFT env variables are present, use the available connection info:
if(process.env.OPENSHIFT_MONGODB_DB_PASSWORD){
  DBurl = process.env.OPENSHIFT_MONGODB_DB_USERNAME + ":" +
  process.env.OPENSHIFT_MONGODB_DB_PASSWORD + "@" +
  process.env.OPENSHIFT_MONGODB_DB_HOST + ':' +
  process.env.OPENSHIFT_MONGODB_DB_PORT + '/' +
  process.env.OPENSHIFT_APP_NAME;
}

var Csummary; // Containts Consumption Summary (Updated every 10 min).

MongoClient.connect(DBurl, function (err, db) {  
    if (err) throw err;
    console.log("Database created !");
    //Updating IOTdevices (At server statup , all devices are disconnected !!!)
    db.collection("IOTdevices").updateMany({}, { $set: { "connected": false } }, { upsert: false , multi: true });
    db.close();
});

//----------------------------------------------------------------------
function DBCountConsumptions(collectionName, date_from, date_to, CB) {

  if (typeof (collectionName) === "string" && collectionName !== ""
    && typeof (date_from) === "string" && date_from !== ""
    && typeof (date_to) === "string" && date_to !== "") {
    MongoClient.connect(DBurl, function (err, db) {
      if (err) throw err;
      db.collection(collectionName).aggregate(

        // Pipeline
        [
          // Stage 1
          {
            $match: {
              $and: [

                { "consumptions.t": { $gte: new Date(date_from) } },
                { "consumptions.t": { $lte: new Date(date_to) } }
              ]
            }
          },

          // Stage 2
          {
            $project: {
              _id: false, "consumptions": {
                $filter: {
                  input: "$consumptions",
                  as: "consumption",
                  cond: {
                    $and: [
                      { $gte: ["$$consumption.t", new Date(date_from)] },
                      { $lte: ["$$consumption.t", new Date(date_to)] },
                    ]
                  }
                }
              }
            }
          },

          // Stage 3
          {
            $project: {
              total: { $sum: "$consumptions.p" }
            }
          },

          // Stage 4
          {
            $group: {
              _id: "total", totals: { $sum: "$total" }
            }
          },

        ],
        function (err, DBresult) {
          db.close();
          // console.log("\nDBCountConsumptions Result from " + date_from + " to " + date_to);
          // console.log(JSON.stringify(DBresult));
          if (typeof (CB) === "function") {
            CB((DBresult.length > 0) ? DBresult[0].totals / (1000 * 3600) : 0); // KW
          }
        });
      /*.then(function (DBresult) {
        db.close();
        console.log("DBCountConsumptions Result :");
        console.log(JSON.stringify(DBresult));
        if (typeof (CB) === "function") { CB(DBresult); }
      });*/
    });
  }

}

function getConsumptionSummary(CB) {
  var s = {
    day: [], // [24] (In this day)
    week: [], // [7] (In this week)
    month: [], // [depends] (In this month)
    year: [], // [12] (In this year)
    dayTotalConsumptions: 0,
    weekTotalConsumptions: 0,
    monthTotalConsumptions: 0,
    yearTotalConsumptions: 0,
  }

  var now = new Date();
  var year = now.getFullYear();
  var month = now.getMonth() + 1;
  var date = now.getDate();


  function daysInMonth(iMonth, iYear) {
    return 32 - new Date(iYear, iMonth, 32).getDate();
  }

  function ymd(dt, m, d, y) {
    if (typeof (m) !== "number") {
      m = "01";
    } else {
      if (m < 10) {
        m = "0" + m;
      }
    }
    if (typeof (d) !== "number") {
      d = "01";
    } else {
      if (d < 10) {
        d = "0" + d;
      }
    }

    if (m === undefined || d === undefined || m > 12 || d > 31) {
      return "";
    }
    if (typeof (dt) !== "string") {
      dt = (dt === true) ? "23:59:59" : "00:00:00";
    }

    // exp : '2013-12-12T16:00:00.000Z'
    return ((y === undefined) ? year : y) + "-" + m + "-" + d + "T" + dt + ".000Z";
    //return ((y === undefined) ? year : y) + "/" + m + "/" + d + " " + dt;
  }

  function From(m, d, y) {
    return ymd(false, m, d, y);
  }

  function To(m, d, y) {
    return ymd(true, m, d, y);
  }

  function getWeek() {
    var date = new Date();
    date.setHours(0, 0, 0, 0);
    // Thursday in current week decides the year.
    date.setDate(date.getDate() + 3 - (date.getDay() + 6) % 7);
    // January 4 is always in week 1.
    var week1 = new Date(date.getFullYear(), 0, 4);
    // Adjust to Thursday in week 1 and count number of weeks from date to week1.
    return 1 + Math.round(((date.getTime() - week1.getTime()) / 86400000 - 3 + (week1.getDay() + 6) % 7) / 7);
  }

  function getDateRangeOfWeek(weekNo) {
    var d1 = new Date();
    numOfdaysPastSinceLastMonday = eval(d1.getDay() - 1);
    d1.setDate(d1.getDate() - numOfdaysPastSinceLastMonday);
    var weekNoToday = getWeek();
    var weeksInTheFuture = eval(weekNo - weekNoToday);
    d1.setDate(d1.getDate() + eval(7 * weeksInTheFuture));
    var rangeIsFrom = eval(d1.getMonth() + 1) + "/" + d1.getDate() + "/" + d1.getFullYear();
    d1.setDate(d1.getDate() + 6);
    var rangeIsTo = eval(d1.getMonth() + 1) + "/" + d1.getDate() + "/" + d1.getFullYear();
    return {
      from: rangeIsFrom,
      to: rangeIsTo
    };
  }

  function rRepeat(k, cb) {
    if (k > 0) {
      cb(k - 1, cb);
    }
  }

  var w = getDateRangeOfWeek(getWeek());
  var tdt = new Date(w.from);

  DBCountConsumptions("IOTdevices", From(1), To(12, daysInMonth(month, year)), function (r) {
    s.yearTotalConsumptions = r;
    DBCountConsumptions("IOTdevices", From(month, 1), To(month, daysInMonth(month, year)), function (r) {
      s.monthTotalConsumptions = r;
      DBCountConsumptions("IOTdevices", w.from, w.to, function (r) {
        s.weekTotalConsumptions = r;
        DBCountConsumptions("IOTdevices", From(month, date), To(month, date), function (r) {
          s.dayTotalConsumptions = r;
          //----------------------------------------
          console.log("Day Counting ...");
          /*DBCountConsumptions("IOTdevices",
            ymd("00:00:00", month, date),
            ymd("23:59:59", month, date),
            function(r) {
              s.day.splice(0, 0, r);
              console.log("r = " + r);
              CB(r);
            });*/

          rRepeat(24, function (k, cb) {
            DBCountConsumptions("IOTdevices",
              ymd(((k < 10) ? "0" + k : k) + ":00:00", month, date),
              ymd(((k < 10) ? "0" + k : k) + ":59:59", month, date),
              function (r) {
                s.day.splice(0, 0, r);
                if (k > 0) {
                  cb(k - 1, cb);
                } else {
                  //----------------------------------------
                  console.log("Week Counting ...");
                  rRepeat(7, function (k, cb) {
                    DBCountConsumptions("IOTdevices",
                      From(month, tdt.getDate()),
                      To(month, tdt.getDate()),
                      function (r) {
                        s.week.push(r);
                        tdt.setTime(tdt.getTime() + (1000 * 60 * 60 * 24));
                        if (k > 0) {
                          cb(k - 1, cb);
                        } else {
                          //----------------------------------------
                          console.log("Month Counting ...");
                          tdt.setDate(1);
                          rRepeat(daysInMonth(month, year), function (k, cb) {
                            DBCountConsumptions("IOTdevices",
                              From(month, tdt.getDate()),
                              To(month, tdt.getDate()),
                              function (r) {
                                s.month.push(r);
                                tdt.setDate(tdt.getDate() + 1);
                                if (k > 0) {
                                  cb(k - 1, cb);
                                } else {
                                  //----------------------------------------
                                  console.log("Year Counting ...");
                                  rRepeat(13, function (k, cb) {
                                    DBCountConsumptions("IOTdevices",
                                      From(k),
                                      To(k, daysInMonth(k, year)),
                                      function (r) {
                                        s.year.splice(0, 0, r);
                                        if (k > 1) {
                                          cb(k - 1, cb);
                                        } else {
                                          CB(s);
                                        }
                                      });
                                  });
                                  //----------------------------------------
                                }
                              });
                          });
                          //----------------------------------------
                        }
                      });
                  });
                  //----------------------------------------
                }
              });
          });
          //----------------------------------------
        });
      });
    });
  });


}
//----------------------------------------------------------------------
function Client(id, ws,  NetworkManager, HandleRequestCB) {

    var self = this;
    this.getTIME_NOW = function () {
        var d = new Date, dformat = [d.getDate(), d.getMonth() + 1, d.getFullYear()].join('/') + ' ' + [d.getHours(), d.getMinutes(), d.getSeconds()].join(':');
        return dformat;
    };
    this.ValidateIPaddress = function (ipaddress) {
        if (/^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/.test(ipaddress)) {
            return (true)
        }
        return (false)
    };
    this.ValidateMACaddress = function (mac) {
        if (/^([0-9a-fA-F][0-9a-fA-F]:){5}([0-9a-fA-F][0-9a-fA-F])$/.test(mac)) {
            return (true)
        }
        return (false)
    };
    this.STR2JSON = function (str) {
        try {
            return JSON.parse(str);
        }
        catch (e) {
            return null;
        }

    };
    this.isset = function (a) {
        if (a === undefined || a === null) { return false; }
        return true;
    };

    if (this.isset(id) === false || this.isset(NetworkManager) === false || typeof (HandleRequestCB) !== "function" || this.isset(HandleRequestCB) === false) { ws.close(); return null; }


    //----  ----  ----  ----  ----  ----  ----  ----  ----  ----  ----
    var connection_time = this.getTIME_NOW();
    var Messages = { Sent: 0, Received: 0 };


    this.infos = { type: "", ip: "", mac: "", geo: "" };
    this.hasInfo = function (a) {
        if (typeof (a) !== "string" || a === "") { return false; }
        return this.infos.hasOwnProperty(a);
    };
    this.set = function (key, value) { this.infos[key] = value; return this; };
    this.get = function (key) { if (this.hasInfo(key)) { return this.infos[key]; } return null; };

    function getConnectionTime() { return connection_time; }
    this.connection_time = function () { return getConnectionTime(); }


    function IncS() { Messages.Sent++; }
    function IncR() { Messages.Received++; }
    function getMessages(key) { return Messages[key]; }
    this.nbSent = function () { return getMessages("Sent"); };
    this.nbReceived = function () { return getMessages("Received"); };
    
    function msg(data) { ws.send(data, function () { }); IncS();}
    this.send = function (data) { console.log("Sending Message To " + id); msg(data); };

    function disconnect() { ws.close(); }
    this.disconnect = function () { disconnect(); };

    this.setIP = function (ip) { if (this.ValidateIPaddress(ip) === true) { this.set("ip", ip); return true; } else { return false; } }
    this.setMAC = function (mac) { if (this.ValidateMACaddress(mac) === true) { this.set("mac", mac); return true; } else { return false; } }

    function getID() { return id; }
    this.getID = function () { return getID(); }

    function renameID(newid) {
        if (typeof (newid) === "string") {
            if (newid !== id) {


                NetworkManager.getClients()[newid] = NetworkManager.getClients()[id];
                delete NetworkManager.getClients()[id];
                id = newid;
                return true;
            }
        }
        return false;
    }
    this.renameID = function (newid) { return renameID(newid); };

    this.OnDisconnect = null;

    ws.on('message', function incoming(message, flags) {
        IncR();
        console.log('received: %s', message);

        if (!flags.binary) {

            //********************************************************	   
            
            var t = message.length;

            if (t > 8) {  // minimal size is 9 , exp : {c:a,d:a}

                var jmsg = self.STR2JSON(message);   // console.log("jmsg = " + jmsg);

                if (self.isset(jmsg)===true) {

                    var command = jmsg['c'];     // console.log("command = " + command);
                    var data = jmsg['d'];        // console.log("data = " + data);

                    if (self.isset(command) === true && typeof (command) === 'string' && command.length>0 && data !== null) {
               
                        HandleRequestCB(self, NetworkManager, command, data);
                        
                    }

                    console.log("\n\n");
                }
                
            }//End of command check test
            //********************************************************

        } //End Of Text Content Handling
        else {
            console.log(">>> Binary Data Received !!! <<<");
        } //End Of binary Content Handling


    });

    ws.on("close", function () {
        if (typeof(self.OnDisconnect) === "function") { self.OnDisconnect(); }
        NetworkManager.Disconnect(id);
    })

    ws.on('pong', heartbeat);
    function heartbeat() { this.isAlive = true; }
}

function Clients() {

    var clients = {};
    var nbclients = 0;
    var lastID = 0;

    function allClients() { return clients; };
    this.getClients = function () { return allClients(); };

    function NbClients() { return nbclients; }
    this.Count = function () { return NbClients(); };

    function AddClient(ws,  NetworkManager, HandleRequestCB) { nbclients++; lastID++; clients["client_" + lastID] = new Client("client_" + lastID, ws,  NetworkManager, HandleRequestCB); }
    this.Connect = function (ws,  HandleRequestCB) { if (isset(ws)) { AddClient(ws, this, HandleRequestCB); } return this; };

    function RemoveClient(id) { delete clients[id]; nbclients--; }
    this.Disconnect = function (id) { if (this.hasClient(id)) { RemoveClient(id); } return this; };

    this.hasClient = function (id) { if (typeof (id) !== "string" || id === "") { return false; } return clients.hasOwnProperty(id); };

    this.sendTo = function (des, data, cb) {

        if (this.hasClient(des) === true) {
            this.getClients()[des].send(data);
            if (typeof (cb) === "function") { cb(true); }
        } else {
            if (typeof (cb) === "function") { cb(false); }
        }

    };

    this.getClient = function (c) {

        if (this.hasClient(c) === true) {
            return this.getClients()[c];
        } else {
            return null;
        }

    };

    this.ForEachCanal = function (CB) {

        if (nbclients > 0 && typeof (CB) === "function") {
            for (c in clients) { CB(this.getClients()[c] ); }
        }
    };


    function isset(a) {
        if (typeof a === undefined || a === null) { return false; }
        return true;
    }
}
//----------------------------------------------------------------------

var WSClients = new Clients();

setInterval(function() {
  getConsumptionSummary(function (summary) {
    Csummary = summary;
    console.log('Consumption Summary Is Updated !');
    
  });
}, 600000); // Updated every 10 min.


getConsumptionSummary(function (summary) {
  Csummary = summary;
  console.log('Consumption Summary Is Loaded !');

  wss.on("connection", function connection(ws, req) {

    WSClients.Connect(ws, canalManager);
    console.log("Websocket Connection Opened " + Date());

  });

});




//----------------------------------------------------------------------

const interval = setInterval(function ping() {
    wss.clients.forEach(function each(ws) {
        if (ws.isAlive === false) return ws.terminate();

        ws.isAlive = false;
        ws.ping('', false, true);
    });
}, 10000);

//----------------------------------------------------------------------

function canalManager(canal, NetworkManager, command, data) {

    // First Message Must Be : {"c":"type","d":"iot"} || {"c":"type","d":"dashboard"}  ( Works only one time )
      
    var c = command.toLowerCase();

    
    if (c === "type" && canal.get("type") ==="") {
        if (typeof (data) === "string") {
            data = data.toLowerCase();
            if (data === "iot" || data === "dashboard") {
                canal.set("type", data);
                if (data === "iot") { canal.renameID(''); /*canal.set("iotID", "");*/ } //Init
                return;
            }
        }
    }
    
    /*
    canal.set("type", "iot");
    canal.set("iotID", "59f6469214b0df22c0428e7a");
    c = "power"; data = 6000;*/
    

    // Checking canal nature
    switch (canal.get("type")) {
        case "iot": IOT(); break;
        case "dashboard": Dashboard(); break;
        default: console.log("Unauthorized Device !!!"); canal.disconnect(); break;
    }

   
    function IOT() {

        var config = {
            iot: [{ key: "iotID", type: "string" }, { key: "on", type: "boolean" }],
            power: "number" // in Watt
        };

        console.log("\nDevice : IOT");

        checkInputs(config , c , function () {

            var iotID = canal.getID(); //var iotID = canal.get("iotID"); 
            console.log("iotID = " + iotID);

            if (iotID === "" || iotID === null) { // The iotID is unknown.
                if (c === "iot") {
                    console.log("\ndata.iotID = " + data.iotID);
                    if (data.iotID === "") {
                        // Create an ID for the device
                        CreateDevice(function (id) {

                            iotCanal(id, data.on);
                            console.log(canal.getID() + " <=> " + typeof( canal.getID() ));
                            send(c, id);
                            SendDevice2Dashboards(id);

                        } , data.on);
                    } else {

                        if (NetworkManager.hasClient(data.iotID) === false) { // Multi connections (canals) are disallowed (iot device <-> 1 connection)

                            console.log("Searching for device in DB ...")
                            FindDevice(data.iotID,
                                function (device) {
                                    if (device !== null) {  

                                        iotCanal(data.iotID, data.on);
                                        send(c, data.iotID); // Sending back the same iotID as ack.
                                        UpdateDevice(data.iotID, { on: data.on, connected: true }, function () {
                                            SendDevice2Dashboards(data.iotID);
                                        });



                                    } else { console.log("Device Rejected !!!"); canal.disconnect(); }   // Not Allowed Client !!!
                                }
                            );

                        } else { console.log("Device Already Connected !!!"); canal.disconnect(); }
                        
                    }
                }
            } else { // The iotID is known.
                
                if (c === "power"  && data > -1 && canal.get("iotON")===true) {
                    console.log("GREAT !!! > " + iotID + " | " + data + " | " + canal.getTIME_NOW() + " | " + typeof (iotID));
                    // Saving new consumption data
                    //SaveConsumption(iotID, data, canal.getTIME_NOW());
                    SaveConsumption(iotID, data, new Date() );  // data (watt in that second)

                }
            }
            
        });
        
    }

    //.. .... .... .... .... .... .... .... ..

    function Dashboard() {

        var config = {
            devices: [], // Getting devices list.
            device: [{ key: "device", type: "string" }, { key: "getRestrictions", type: "boolean" }, { key: "getTasks", type: "boolean" }], // Getting device infos.
            rename: [{ key: "device", type: "string" }, { key: "name", type: "string" }], // Renaming device

            manual: [{ key: "devices", type: "object" }, { key: "on", type: "boolean" }], // Turning ON/OFF.
            shortcut: [{ key: "devices", type: "object" }, { key: "name", type: "string" }],
            auto: [{ key: "devices", type: "object" }, { key: "on", type: "boolean" }, { key: "at", type: "string" }], // Scheduling task(s).
            protect: [
                { key: "devices", type: "object" },
                { key: "name", type: "string" },
                { key: "on", type: "boolean" },
                { key: "from", type: "string" },
                { key: "to", type: "string" }
            ],

            unprotect: [{ key: "devices", type: "object" }, { key: "restrictions", type: "object" }], // Arrays of string

            consumption: [
                { key: "devices", type: "object" },
                { key: "filter", type: "string" }, //"year - mounth - day"
                { key: "from", type: "string" },
                { key: "to", type: "string" }
            ],

            csummary: [],

        };

        console.log("\nDevice : DASHBOARD");

        checkInputs(config, c, function () {

          if (['device', 'devices', 'rename', 'csummary'].indexOf(c) > -1) {

              switch (c) {
                case "device":
                  break;

                case "devices":
                  getDevicesList(function (list) {
                    console.log("\n\nDEVICES LIST :\n\n");
                    console.log(list);
                    send(c, list);
                  });
                  break;

                case "rename":
                  console.log(`\n\nRenaming device ${data.name} to ${data.name}\n\n`);
                  UpdateDevice(data.device, { name: data.name }, function () {
                    SendDevice2Dashboards(data.device,false); //ID,false: send to all dashboards , no exeptions.
                  });
                  break;


                case "csummary":
                    send(c, Csummary);
                  break;


                default:
                break;
              }
                
            } else {

                    // Works only if data.devices contains at least one element.
                var targetCanal;

                ForEachDevice(data.devices, function (deviceID,device) {  // Stoped here
                    targetCanal = NetworkManager.getClient(deviceID);

                        switch (c) {

                            case 'manual':
                                if (data.on !== targetCanal.get("iotON")) {
                                    targetCanal.send(NewMsg("turn", data.on));
                                    targetCanal.set("iotON", data.on); 
                                    UpdateDevice(deviceID, { on: data.on } , function() {
                                        SendDevice2Dashboards(deviceID);
                                    });
                                }
                                
                                break;

                            case 'shortcut':
                                break;
                            case 'auto':
                                break;
                            case 'protect':
                                break;
                            case 'unprotect':
                                break;
                            case 'consumption':
                                break;

                            default: break;
                        }

                });
                
            }
            
        });
    }
    
    //.. .... .... .... .... .... .... .... ..

    function NewMsg(command, data) {
        return JSON.stringify({ "c": command,"d":data});
    }

    function send(command, data) {
        canal.send(NewMsg(command, data) );
    }

    function jsformat(data, mask) {

        if (typeof (data) === "object" && typeof (mask) === "object") {

            var i; var t = mask.length;
            for (i = 0; i < t; i++) {
                if (mask[i].key !== undefined && mask[i].type !== undefined) {
                    if (typeof (data[mask[i].key]) !== mask[i].type) { return false; }
                }
            }
            return true;
        }

        if (typeof (mask) !== "string") { return false; }
        if (typeof (data) !== mask) { return false; }

        return true;
    }

    function checkInputs(configOBJ, key, cb) {

        if (configOBJ.hasOwnProperty(key) === true) {

            if (jsformat(data, configOBJ[key]) === true) {
                console.log("[ " + key + " ] <=> " + JSON.stringify(data)+"\n\n");
                cb();
            }
        }
    }

    function Send2Dashboards(command, data ,bSelfExeption) {
        var msg = NewMsg(command, data);
        NetworkManager.ForEachCanal( function (L_canal) {
            
            if (L_canal.get("type") === "dashboard") {
                console.log(`\nSending "${command}" to ${L_canal.getID()} [ ${ L_canal.get("type")} ]`);
                if (L_canal.getID() === canal.getID() && bSelfExeption === true) { return; }
                L_canal.send(msg);
            }
        });
    }

    function SendDevicesList2Dashboards() {

        getDevicesList(function (list) {
            console.log("\n\nSending Devices List To Dashboards .... ");
            console.log(list);
            Send2Dashboards("devices", list, true); // Refreshing devices list (case if a device is removed from DB)!!!
            console.log("DONE !!!\n\n");
        });
    }

    function SendDevice2Dashboards(iotID, bSelfExeption) {
      if (typeof (bSelfExeption) !=="boolean") {bSelfExeption = true;}
      FindDevice(iotID, function (d) {
          Send2Dashboards("device", d, bSelfExeption);
        });
    }

    //.. .... .... .... .... .... .... .... ..

    function CreateDevice(onDoneCB, on) {

        //Creating new device in database (Saving current device status on/off )
        DBInsert("IOTdevices",

            {
                name: "New Device",
                connected:true,
                on: on,
                tasks: [],
                restrictions: [],
                consumptions: [],
                created_at: new Date()
            },

            function (DBresult) {

                //Sending device ID to the callback function
                onDoneCB(DBresult.insertedId.toString());
            }
        );

    }

    function FindDevice(iotID, searchCB) {
        DBFindByID("IOTdevices", iotID, searchCB);
    }

    function UpdateDevice(iotID, updateOBJ, CB , options) {
        DBFindByIDAndUpdate("IOTdevices", iotID, { $set: updateOBJ }, CB, options);
    }

    function SaveConsumption(iotID, data, time, CB, options) {
        console.log("******************************");
        DBFindByIDAndUpdate("IOTdevices", iotID, { $addToSet: { consumptions: { p: data, t: time } } }, CB, { upsert: true});
    }

    function iotCanal(iotID,bON) {
        canal.OnDisconnect = function () {
            console.log(`Updating device "${iotID}" <-> connected to 'false'.`);
            UpdateDevice(iotID, { connected: false }, function (d) {
                SendDevice2Dashboards(iotID);
            });
        };
        canal.renameID(iotID); // canal.set("iotID", data.iotID);
        canal.set("iotON", bON); 
    }
    //.. .... .... .... .... .... .... .... ..
    
    function getDevices(b_tasks, b_consumptions, b_restrictions, CB) {

        var filterOBJ = {};
        if (b_tasks === false) { filterOBJ["tasks"] = false; }
        if (b_consumptions === false) { filterOBJ["consumptions"] = false; }
        if (b_restrictions === false) { filterOBJ["restrictions"] = false; }

        DBGetAll("IOTdevices", filterOBJ , CB);
    }

    function getDevicesList(CB) { getDevices(true, false, true, CB); }
    function getAllTasks(CB) { getDevices(true, false, false, CB); }
    function getAllConsumptions(CB) { getDevices(false, true, false, CB); }
    function getAllRestrictions(CB) { getDevices(false, false, true, CB); }

    //.. .... .... .... .... .... .... .... ..

    function ForEachDevice(devicesArray, CB) {

        if (Array.isArray(devicesArray) === true && typeof(CB) === "function") {
            if (devicesArray.length > 0) {
                for (i in devicesArray) {
                    
                    FindDevice(devicesArray[i], function (device) {
                        if (device !== null) {
                            if (device.connected === true) {
                                CB(devicesArray[i], device);
                            }
                        }
                    });
                    
                }
            }
        }

    }
    //.. .... .... .... .... .... .... .... ..

    function DBInsert(collectionName, OBJ , CB) {

        if (typeof (collectionName) === "string" && collectionName !=="" && typeof (OBJ) === "object" && OBJ !== null && OBJ !== undefined) {
            MongoClient.connect(DBurl, function (err, db) {
                if (err) throw err;
                db.collection(collectionName).insertOne(OBJ)
                    .then(function (DBresult) {
                        db.close(); 
                        console.log(JSON.stringify(DBresult));
                        if (typeof (CB) === "function") { CB(DBresult); }
                });
            });
        }
        
    }

    function DBFindByID(collectionName, id, CB) {
        console.log("Searching for id : "+id);
        if (typeof (collectionName) === "string" && collectionName !== "" && typeof (id) === "string" && id !== "") {
            MongoClient.connect(DBurl, function (err, db) {
                if (err) throw err;
                db.collection(collectionName).findOne({ _id: new ObjectId(id) })
                    .then(function (DBresult) {
                        db.close();
                        console.log(JSON.stringify(DBresult));
                        if (typeof (CB) === "function") { CB(DBresult); }
                    });
            });
        }

    }

    function DBFindAndUpdate(collectionName, OBJ, NEWOBJ, CB, options) {
        console.log("collectionName : " + collectionName);
        console.log(OBJ);
        console.log(NEWOBJ);

        if ( typeof (collectionName) === "string" && collectionName !== ""
             && typeof (OBJ) === "object" && OBJ !== null
            && typeof (NEWOBJ) === "object" && NEWOBJ !== null) {

            //console.log("Ok !!!");

            if (options === undefined || options === null) { options = {}; }

            MongoClient.connect(DBurl, function (err, db) {
                if (err) throw err;
                db.collection(collectionName).findOneAndUpdate(OBJ, NEWOBJ, options)
                    .then(function (DBresult) {
                        db.close();
                        //console.log(JSON.stringify(DBresult));
                        if (typeof (CB) === "function") { CB(DBresult); }
                    }).catch(function (err) { console.log("ERR : " + JSON.stringify(err)); });
            });
        }

    }

    function DBFindByIDAndUpdate(collectionName, id, NEWOBJ, CB, options) {

        //console.log(JSON.stringify(id));
        //console.log("=>>>> id = " + id + " type = " + typeof(id));
        if (typeof(id) === "string" && id !== "") {
            //console.log("ID : ok !!!");
            DBFindAndUpdate(collectionName, { _id: new ObjectId(id) }, NEWOBJ, CB , options);
        }
    }

    //.. .... .... .... .... .... .... .... ..

    function DBFindAsArray(collectionName , QueryOBJ , filterOBJ , CB) {

        /*  EXP :
            find({"_id":{$in:[ObjectId("59f675a7e564ae20d466852b"),ObjectId("59f676565f598a2838987e19")]}},{tasks:false,restrictions:false})
        */
        
        if (typeof (collectionName) === "string" && collectionName !== "" &&
            typeof (QueryOBJ) === "object" && QueryOBJ !== null && QueryOBJ !== undefined &&
            typeof (filterOBJ) === "object" && filterOBJ !== null && filterOBJ !== undefined) {

            MongoClient.connect(DBurl, function (err, db) {
                if (err) throw err;
                db.collection(collectionName).find(QueryOBJ, filterOBJ).toArray(function (err , DBresult) {
                        if (err) throw err;
                        db.close();
                        console.log(JSON.stringify(DBresult));
                        if (typeof (CB) === "function") { CB(DBresult); }
                    });
            });
        }

    }

    function DBGetAll(collectionName , filterOBJ , CB) {
        DBFindAsArray("IOTdevices", {}, filterOBJ, CB);
    }

    //.. .... .... .... .... .... .... .... ..
 
//------------------------------------------------------------------------------------

}