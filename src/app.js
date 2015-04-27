var cluster = require('cluster');
var fs = require('fs');
var worker;
var fulllog = {};
var cpuCount = 4; // for the challenge

if (cluster.isMaster){

  for (var i = 0; i < cpuCount; i += 1) {
    worker = cluster.fork();
    
    worker.on('message',function(msg){
      if (msg.result){
        mergeLogs(msg.result);
      }
    });
  } 


} else {
  var log = {};
  
  console.log('Worker ',cluster.worker.id, ' running!');
  
  var filename = 'mock/logs/server'+cluster.worker.id+'/log.txt';

  fs.readFile(filename,'utf8',function(err,data){
    if (err) throw err;
    console.log('OK > ',filename);
    console.log(data);
    logParser(data,function(log){
      process.send({result:log});
    });
  });

}

function createFiles(){
  if (!fs.existsSync('tmp')) {
    console.log('---- created diretory ----');
    fs.mkdirSync('tmp');
  }

  for (id in fulllog){
    var filePath = 'tmp/'+id+'.txt';
    fs.writeFileSync(filePath,'');
    fulllog[id].forEach(function(line){
      fs.appendFileSync(filePath,line+'\n');
    });
  }

  console.log('End');
  process.exit();
}

var count = 0;
function mergeLogs(log){
  count++;
  for (id in log){
    if (!fulllog[id]){
      fulllog[id] = [];
    }
    log[id].forEach(function(logline){
      fulllog[id].push(logline);
    });
  }

  if (count === cpuCount){
    reorder(fulllog);
    console.log(fulllog);
    createFiles();
  }
}

function reorder(log){
  for(id in log){
    log[id].sort(function(a,b){
      var timeA = new Date(a.substring(a.indexOf('[') + 1,a.indexOf(']'))).getTime();
      var timeB = new Date(b.substring(b.indexOf('[') + 1,b.indexOf(']'))).getTime();
      return timeA - timeB;
    });
  }
}

function logParser(data,callback){
  var log = data.split('\n');
  var result = {};
  log.forEach(function(line){
    if (line){
      //console.log(line);
      var userid = line.split('userid=')[1];
      userid = userid.substring(0,userid.length -1);
      if(!result[userid]){
        result[userid] = [];
      }
      result[userid].push(line);
    }
  });

  callback(result);
}

