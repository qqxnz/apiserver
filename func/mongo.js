/**mongodb功能
创建时间：2016-09-23
创建人：吕扶美

更新时间：2016-12-20 11:39:32
更新内容： 
        result = false; 全都改成result = null;
        优化细节
更新人：钟宝森

更新时间：2017-6-16 11:47:24
更新内容： 
        修正updateone函数条件
        添加find查询条件限制
更新人：钟宝森

*/
var Fiber = require('fibers');
var poolModule = require('generic-pool');
var sprintf = require("sprintf-js").sprintf;
var mongoClient = require('mongodb').MongoClient;
var mongodb = require('mongodb');
var config = require('./config.js');



// var host1 = "dds-bp15bd02fc2165841.mongodb.rds.aliyuncs.com";
// var port1 = 3717;
// var host2 = "dds-bp15bd02fc2165842.mongodb.rds.aliyuncs.com";
// var port2 = 3717;

// var host1 = conf.mongodb.host1;
// var port1 = 3717;
// var host2 = "120.27.216.223";
// var port2 = 37171;
// var username = "root";
// var password = "Zyk123456";
// var replSetName = "mgset-2358551";

// var dbName = "test";


var mongo = {};

// mongo.pool = poolModule.Pool({
//     name: 'mongodb',
//     //将建 一个 连接的 handler
//     create: function(callback) {
//         var conf = config.get('app');
//         // console.log('MongoDB连接池创建连接!');
//         var url = 'mongodb://'+ conf.mongodb.address +':'+ conf.mongodb.port +'/'+ conf.mongodb.db;
//         var  server = mongodb.Server(conf.mongodb.address, conf.mongodb.port, {auto_reconnect:false});
//         var  db = mongodb.Db(conf.mongodb.db, server, {safe:true});
//         db.open(function(err,result){  //注意，必须打开数据库，然后登录验证
//             db.authenticate(conf.mongodb.user,conf.mongodb.pass,function(err,result){
//                 if(!result){
//                     callback(err);
//                 }else{
//                     callback(null,db);
//                 }
//             });
//         });

//     },
//     // 释放一个连接的 handler
//     destroy  : function(db) { 
//         // console.log('MongoDB连接池释放连接!');
//         db.close();
//     },
//     // 连接池中最大连接数量
//     max      : 50,
//     // 连接池中最少连接数量
//     min      : 10, 
//     // 如果一个线程3秒钟内没有被使用过的话。那么就释放
//     idleTimeoutMillis : 30000,
//     // 如果 设置为 true 的话，就是使用 console.log 打印入职，当然你可以传递一个 function 最为作为日志记录handler
//     log : true 
// });





mongo.pool = poolModule.Pool({
    name: 'mongodb',
    //将建 一个 连接的 handler
    create: function (callback) {
        var conf = config.get('app');
        var url = 'mongodb://' + conf.mongodb.user + ':' + conf.mongodb.pass + '@' + conf.mongodb.address + ':' + conf.mongodb.port + '/' + conf.mongodb.db;
        mongoClient.connect(url, function (err, db) {
            callback(err, db);
        });
    },
    // 释放一个连接的 handler
    destroy: function (db) { db.close(); },
    // 连接池中最大连接数量
    max: 50,
    // 连接池中最少连接数量
    min: 10,
    // 如果一个线程30秒钟内没有被使用过的话。那么就释放
    idleTimeoutMillis: 30000,
    // 如果 设置为 true 的话，就是使用 console.log 打印入职，当然你可以传递一个 function 最为作为日志记录handler
    log: false
});





mongo.open = function (cb) {
    mongo.pool.acquire(function (err, db) {
        cb(err, db);
    });
}



mongo.close = function (db) {
    mongo.pool.release(db);
}

mongo.save = function (db, table, data) {
    var result = {};
    var fiber = Fiber.current;
    db.collection(table).save(data, function (err, data) {
        if (err) {
            result = null;
            fiber.run();
        } else {
            result = data;
            fiber.run();
        }
    });
    Fiber.yield();
    return result;
}


mongo.insertMany = function (db, table, dataArray) {
    var result = {};
    var fiber = Fiber.current;
    db.collection(table).insertMany(dataArray, function (err, data) {
        if (err) {
            result = null;
            fiber.run();
        } else {
            result = data;
            fiber.run();
        }
    });
    Fiber.yield();
    return result;
}


mongo.updateOne = function (db, table, data, where) {
    var result = {};
    var fiber = Fiber.current;
    db.collection(table).update(data, { $set: where }, function (err, data) {
        if (err) {
            result = null;
            fiber.run();
        } else {
            result = data;
            fiber.run();
        }
    });
    Fiber.yield();
    return result;
}

mongo.deleteOne = function (db, table, where) {
    var result = {};
    var fiber = Fiber.current;
    db.collection(table).deleteOne(where, function (err, data) {
        if (err) {
            result = null;
            fiber.run();
        } else {
            result = data;
            fiber.run();
        }
    });
    Fiber.yield();
    return result;
}

mongo.find = function (db, table, data, ts) {
    var result = {};
    var fiber = Fiber.current;
    if (ts != '' && ts != null && ts != undefined) {
        db.collection(table).find(data).limit(ts).toArray(function (err, data) {
            if (err) {
                result = null;
                fiber.run();
            } else {
                result = data;
                fiber.run();
            }
        });
    } else {
        db.collection(table).find(data).toArray(function (err, data) {
            if (err) {
                result = null;
                fiber.run();
            } else {
                result = data;
                fiber.run();
            }
        });
    }

    Fiber.yield();
    return result;
}


module.exports = mongo;