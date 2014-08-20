"use strict";

/* see http://chrislarson.me/blog/install-neo4j-graph-database-ubuntu
The database can be cleared out by deleting the folder it is stored in.
cd /var/lib/neo4j/data
sudo rm -rf graph.db/
Restart neo4j
sudo /etc/init.d/neo4j-service restart
*/


var neo4j = require('node-neo4j');
var neodb = new neo4j('http://localhost:7474');

var str2json = require ('string-to-json');
var extend = require('extend');
var O = require('observed'); //cool see https://www.npmjs.org/package/observed


var O = require('observed')
//var object = { leiID:{}, countryID:{}, cityID:{} }
var curIDset = { id:{} }
var ee = O(curIDset)

ee.on('add', console.log);
ee.on('new', console.log);
ee.on('change', console.log);
ee.on('update', console.log);
curIDset.leiID = 899;


var events = require('events');
var ev = new events.EventEmitter();

var saveCtr = 0;
var tmpObj={};

ev.on('customEvent', function (data, fn) {
    console.log ("this data just in:" ,data);
    var tmpAr = data.split(":");
    tmpAr[0]= S(tmpAr[0]).trim().s;
    tmpAr[1]= S(tmpAr[1]).trim().s;
    addLabelToThis(tmpAr[1],tmpAr[0]);//Works??
    if (tmpAr[0] == 'leiID') tmpObj.leiID = tmpAr[1];
    if (tmpAr[0] == 'countryID') tmpObj.countryID = tmpAr[1];
    if (tmpAr[0] == 'cityID') tmpObj.cityID = tmpAr[1];
    if (tmpAr[0] == 'hqcityID') tmpObj.hqcityID = tmpAr[1];
    if (tmpAr[0] == 'hqcountryID') tmpObj.hqcountryID = tmpAr[1];
    curIDset = merge (curIDset,tmpObj);   
    saveCtr++;
     if (saveCtr > 4) {
         //console.log("saveCtr", saveCtr);
         //console.log("curIDset", curIDset);
         saveCtr = 0;
         //buildRelations(); //now that all the nodes are there ...
       //*********UNCOMMENT THAT
        };

});





//ee.on('change', console.log);


//curIDset.name.last = 'observed';

//ee.deliverChanges();

//var ee = require('event-emitter');
//var emitter = ee({}), listener;
var EventEmitterGrouped = require('event-emitter-grouped').EventEmitterGrouped;

// Instantiate a new instance
var emitter = new EventEmitterGrouped();


var S = require('string'); //see http://stringjs.com useful string conversions
var CSV = require('csv-string');
var Lazy = require('lazy.js');
var jsesc = require('jsesc');
var asciiJSON = require('ascii-json');
var path = require('path');
var merge = require('merge');//https://www.npmjs.org/package/merge
var readLineSync = require('readlinesync') //https://gist.github.com/Basemm/9700229

var src = 'pleiFull_20140814';
var csvDir = '/home/evengers/Documents/projects/CSVsToLoad';
var theCsv = path.join(csvDir, src +'.csv');

var co = require('co');
var request = require('co-request');

var Q = require('q');
var db = require("seraph")("http://localhost:7474");

/* default Seraph opions
var defaultOptions = {
    // Location of the server
    server: 'http://localhost:7474'
    // datbase endpoint
  , endpoint: '/db/data'
    // The key to use when inserting an id into objects. 
  , id: 'id'
}, optionKeys = Object.keys(defaultOptions);
*/


function addLabelToThis(nodeID, aLabel) {

var selfProperty = nodeID;
// just adding the label 

 var theurl = 'http://localhost:7474/db/data/node/'+ selfProperty + '/labels';

//just checking on the labels
co(function* () {
  var result = yield request({
    headers: {"Content-Type" : "application/json"},
    uri: theurl,
    method: 'POST',
    body: aLabel
  });


//just checking on the labels

  var result = yield request({
    headers: {"Content-Type" : "application/json"},
    uri: theurl,
    method: 'GET'
  });
  var response = result;
  var body = result.body;
 console.log('the labels Body: ', body);
})();

};



function cypherThis(withLabel, indexKeyValandOtherPrams) {

var bodyAsObj = indexKeyValandOtherPrams;
 var bodyAsStr = JSON.stringify(bodyAsObj); 

var thelabel = withLabel;

co(function* () {
 
  var body = bodyAsStr;

 var theurl = 'http://localhost:7474/db/data/index/node/'+ thelabel + '?uniqueness=get_or_create';


  var result = yield request({
    headers: {"Content-Type" : "application/json"},
    uri: theurl,
    method: 'POST',
    //body: testbody
    body: bodyAsStr
  });
  var response = result;
  var body = result.body;
 // var tmpStore=JSON.stringify(body);
  var tmpStore=JSON.parse(body);
  var tmpStore = Lazy(tmpStore).values();
  var anArray = Lazy(tmpStore).toArray();
  var last = anArray.length -1;
  var tmpStore  = anArray[last].split("/");
  var last = tmpStore.length -1;
  var selfProperty = tmpStore[last];
 // console.log('Response: ', response); // may need some of this for reply to client
  console.log('Body: ', body);
  console.log('the ID: ', selfProperty);



  ev.emit('customEvent', "testid:"+selfProperty, function(){});
  
  //reply (body);
})();
  
};

function stateMe(aStr) {
var useEm = {
statements: [{statement: aStr, parameters: {props: {}}}]};
return useEm;
};

function fireTransaction(theStatements) {

//start with the first statement
var useEm = stateMe(theStatements[0]);
var transactionID;

neodb.beginTransaction(useEm, function (err, result){ 
      if (err) { 
             console.log ("got this error on transaction", err);
         }else{
       console.log(result);
       transactionID = result._id;
          //add the rest of the statements
          var len = theStatements.length;
              for (var i=1; i<len; i++){
              var useEm = stateMe(theStatements[i]);
              console.log ("adding this statement to transaction", useEm);
              neodb.addStatementsToTransaction(transactionID, useEm, function  (err, result) { 
 if (err) console.log ("got this error adding statement", err, "  ", useEm);
             });
              }//end for loop

          neodb.commitTransaction(transactionID, function (err, result){ 
             if (err) console.log ("got this error on commit", err);
             console.log(result);
            ee.emit("relationsBuilt");
            })
       }//end if
    })//end beginTrans... 

};


function fireTransactionX(theStatements) {
//console.log ("sending transaction");
var bodyAsObj = {};
bodyAsObj.statements = theStatements;

var tmpStr = JSON.stringify(bodyAsObj);
bodyAsObj = JSON.parse(tmpStr);
console.log (bodyAsObj);

var bodyAsStr = JSON.stringify(bodyAsObj); 

//console.log("the strinigfied one .... ",  bodyAsStr);


/*

var thelabel = 'CITY';
var bodyAsObj = { "statements" : [ {
      "statement" : "CREATE (n {props}) RETURN n", 
      "parameters" : { 
         "props" : { 
            "name": "My Node" 
               } 
             }
            } ] 
     };
 
var bodyAsObj = { "statements" : [ {
      "statement" : 'MERGE (n:Page { url: "http://www.neo4j.org" })', 
"statement1" : 'MERGE (s:StyleSheet { url: "http://www.neo4j.org/styles/main.css" })',
"statement2" : 'MERGE (n)-[:CONTAINS_STYLESHEET]->(s)',
"statement3" : 'RETURN *',
      "parameters" : { 
         "props" : { 
            "name": "My Node" 
               } 
             }
            } ] 
     };
*/



//NO COMMIT HERE 
co(function* () {
  var theurl = 'http://localhost:7474/db/data/transaction';
  var body = bodyAsStr;

  var result = yield request({
    headers: {"Content-Type" : "application/json"},
    uri: theurl,
    method: 'POST',
    //body: testbody
    body: bodyAsStr
  });
 
var response = result;
var body = result.body;
var anObj = JSON.parse(body);
var thecommiturl = anObj.commit;


var errStr = anObj.errors;
var numOfError =1;
if (typeof errstr == "undefined") numOfError =0;
  console.log('test this Body just num of errors : ', numOfError);
  if (numOfError != 0) {
          logRejects ("line: " + ctr + "label prob ");
          console.log('here are the f... errors : ', errStr);
         }else{
             //since it seemed to work .... commit it
             console.log("comit url",thecommiturl);
                 var result = yield request({
                      headers: {"Content-Type" : "application/json"},
                      uri: thecommiturl,
                       method: 'POST',
                    });
                   var body = result.body;
                   var anObj = JSON.parse(body);
                   console.log(body);
                   ee.emit("relationsBuilt");
         };

//now commit



})();

  
  
//now do the Commit

 



 // var anObj = JSON.parse(body);
//  console.log('test this Body id : ', anObj);
//  console.log('test this Body just results : ', anObj.results);
 // console.log('test this Body just commit : ', anObj.commit);




};



function indexThis(req, reply) {

var thelabel = 'CITY';
var bodyAsObj = {"label" : "CITY", "property_keys": ["cityCountry"]};
 var bodyAsStr = JSON.stringify(bodyAsObj); 



co(function* () {
  var theurl = 'http://localhost:7474/db/data/schema/' + thelabel;
  var body = bodyAsStr;

  var result = yield request({
    headers: {"Content-Type" : "application/json"},
    uri: theurl,
    method: 'POST',
    //body: testbody
    body: bodyAsStr
  });

//now get the indexes for the label ... not needed ... just checking
 var result = yield request({
    headers: {"Content-Type" : "application/json"},
    uri: theurl,
    method: 'GET',
  });

  var response = result;
  var body = result.body;
  console.log('Body: ', body);


//now put a constraint on 
theurl = theurl + "constraint/" + thelabel + "/uniqueness/";
var bodyAsObj = {"property_keys": ["cityCountry"]};
 var bodyAsStr = JSON.stringify(bodyAsObj); 

 var result = yield request({
    headers: {"Content-Type" : "application/json"},
    uri: theurl,
    method: 'POST',
    body: bodyAsStr
  });

//get all the constraints
 var result = yield request({
    headers: {"Content-Type" : "application/json"},
    uri: theurl,
    method: 'GET',
  });
  var response = result;
  var body = result.body;
  console.log('These constraints are on Body: ', body);




 /*
  var tmpStore=JSON.parse(body);
  var tmpStore = Lazy(tmpStore).values();
  var anArray = Lazy(tmpStore).toArray();
  var last = anArray.length -1;
  var tmpStore  = anArray[last].split("/");
  var last = tmpStore.length -1;
  var selfProperty = tmpStore[last];
 
  console.log('Body: ', body);
  console.log('the ID: ', selfProperty);
  ev.emit('customEvent', "testid:"+selfProperty, function(){});
  */
  
})();
  
};





var req = {"payload" : "test"}; var reply = null;
var indexKeyValandOtherPrams = {"key" : "countryCode",
                    "value" : "US",
                     "datatype" : "testdata"};

// EXAMPLE cypherThis("COUNTRY", indexKeyValandOtherPrams);
// EXAMPLE  indexThis(null,null);


var fs = require('fs-extra'); //var fs = require('fs')


// any node labelled Person should have a unique `name`
db.constraints.uniqueness.create('COUNTRY', 'countryCode', function(err, constraint) {
  console.log(constraint); 
});

// any node labelled Person should have a unique `name`
db.constraints.uniqueness.create('CITY', 'cityCountry', function(err, constraint) {
  console.log(constraint); 
});


db.index.createIfNone('COUNTRY', 'countryCode', function(err, index) {
  console.log(index); // -> { label: 'Person', { property_keys: ['name'] }
});

db.index.createIfNone('CITY', 'cityCountry', function(err, index) {
  console.log(index); // -> { label: 'Person', { property_keys: ['name'] }
});

function stringsNoKeyQuotes (anObj){
  //neo statements seem to die if quotes on key so JSON.stringify does not work ... this function solves that by returning string 
  var aStr ="{";
   Object.keys(anObj).forEach(function(key){
      var value = anObj[key]; 
      aStr = aStr + key+':"'+ value + '", ';
   });
    aStr = S(aStr).trim().s;
    aStr = S(aStr).chompRight(',').s;
    aStr = aStr + "}";
  return aStr;
  };

function populateStatementsXX(inmapped){

var cityparam = stringsNoKeyQuotes(mapped.city);
var hqcityparam = stringsNoKeyQuotes(mapped.hqcity);
var countryparam = stringsNoKeyQuotes(mapped.country);
var hqcountryparam = stringsNoKeyQuotes(mapped.hqcountry);
var cityparam = stringsNoKeyQuotes(mapped.city);
var leiparam = stringsNoKeyQuotes(mapped.lei);


var theStatements = {
     statements: [
     {savecity : "MERGE (ci:CITY "+ cityparam +" )"},
     {savecountry : "MERGE (co:COUNTRY "+ countryparam +" )"},
     {savelei : "MERGE (le:LEI "+ leiparam +")"},
     {savehqcity : "MERGE (hci:CITY "+ hqcityparam +")"},
     {savehqcountry : "MERGE (hco:COUNTRY "+ hqcountryparam +")"},
     {rcitycountry : "MERGE (ci)-[:IN_COUNTRY]->(co)"},
     {rleicity : "MERGE (le)-[:IN_CITY]->(ci)"},
     {rleicountry : "MERGE (le)-[:IN_COUNTRY]->(co)"},
     {rleihqcity : "MERGE (le)-[:IN_CITY]->(hci)"},
     {rleihqcountry : "MERGE (le)-[:IN_COUNTRY]->(hco)"},
     {theEnd : "RETURN *"}
                  ]};

    fireTransaction(theStatements);
};


function populateStatements(inmapped){

var cityparam = stringsNoKeyQuotes(mapped.city);
var hqcityparam = stringsNoKeyQuotes(mapped.hqcity);
var countryparam = stringsNoKeyQuotes(mapped.country);
var hqcountryparam = stringsNoKeyQuotes(mapped.hqcountry);
var cityparam = stringsNoKeyQuotes(mapped.city);
var leiparam = stringsNoKeyQuotes(mapped.lei);


var theStatements = [
     "MERGE (ci:CITY "+ cityparam +" )",
     "MERGE (co:COUNTRY "+ countryparam +" )",
     "MERGE (le:LEI "+ leiparam +")",
     "MERGE (hci:CITY "+ hqcityparam +")",
     "MERGE (hco:COUNTRY "+ hqcountryparam +")",
     "MERGE (ci)-[:IN_COUNTRY]->(co)",
     "MERGE (le)-[:IN_CITY]->(ci)",
     "MERGE (le)-[:IN_COUNTRY]->(co)",
     "MERGE (le)-[:IN_CITY]->(hci)",
     "MERGE (le)-[:IN_COUNTRY]->(hco)",
     "RETURN *"];

    fireTransaction(theStatements);
};





var isHeader = true;
var rowAsArray = [];
var headerAsArray = [];

var currentLine ="";

var mapped = {};

var nL = '\n';
var Cr = '\r';
var ctr=0;
var file = readLineSync(theCsv);
var next = file.next(); //that reads the first line


function getHeader() {
    var theHeaderArray = [];
    theHeaderArray = S(next.value).parseCSV(); //copes with embedded commas
      Lazy(theHeaderArray).each(trimThem) // trim each header item
      //console.log (theHeaderArray); 
return theHeaderArray;
};


function saveTheCityXx(themappedfields,targetID) {
var deferred = Q.defer();

//console.log("this db connection happened:  ",db);
var City = model(db, 'cityCountry');
//var themappedfields = {tester:"thetest"};
City.save(themappedfields, function (error, text) {
    if (error) {
        deferred.reject(new Error(error));
    } else {
        deferred.resolve(text);
        targetID=text.id;
        console.log ("city id is: ", targetID);
        //deferred.notify("City done");
    }
});

return deferred.promise;
};


function nowAddLabel(theLabels,node){
  db.label(node, theLabels, function(err) {     
     db.read(node.id, function(err, node) {
         console.log(node) 
  })
})
};


// Bind an asynchronous event
emitter.on('saveHappened', function(results){
    var huh = results;
    console.log(huh);
});


function saveTheNodeWithLabel(themappedfields,targetID,theLabels) {
db.save(themappedfields, function(err, node) {
    if (err) logRejects ("line: " + ctr + "node save prob ");
    targetID = targetID + ": "+node.id ;
     db.label(node, theLabels, function(err) {  
         if (err) logRejects ("line: " + ctr + "label prob ");   
 //OPTIONAL    db.read(node.id, function(err, node) {
         ev.emit('customEvent', targetID, function(){});
  //OPTIONAL      });//read  
      })//label
    })//db.save
 };  //end saveTheNodes


//console.log("this db connection happened:  ",db);
var model = require('seraph-model');
var Lei = model(db, 'lei');
var City = model(db, 'cityCountry');
 var Country = model(db, 'country');

function setIndicesOnFirstRun(){
  City.setUniqueKey('cityCountry', true);
   Lei.setUniqueKey('leiID', true);
 Country.setUniqueKey('countryCode', true);
};


function prepareRow() {

   //get next line
    currentLine ="";
    curIDset= {};
    next = file.next();
    var csvline = next.value;

 ctr= ctr+1;
     rowAsArray = S(csvline).parseCSV(); //copes with embedded commas 
            //wraps single quote see https://www.npmjs.org/package/string
      Lazy(rowAsArray).each(trimThem) // trim & adjust each row item
      
      var resArray = Lazy(headerAsArray).zip(rowAsArray); //uses the header as keys for row
      var csvObj = Lazy(resArray).toObject(); 
    // mappedFields = {};

     mapped=updateMapWith(csvObj,ctr); //does the mapping
     //dotheseSaves(); //save and get id for each node for each row
     populateStatements(mapped); //*********
 //when that is complete the the relationship build starts
};

function logRejects ( text ) 
{     
  fs.open('rejects.txt', 'a', function( e, id ) {
   fs.write( id, text + "\n", null, 'utf8', function(){
    fs.close(id, function(){
     console.log('reject file is updated');
    });
   });
  });
 };

//helper for trim
function trimThem(x) { 
    x = S(x).replaceAll(', ,', ',').s; //get rid of stray commas
    x = S(x).replaceAll(',,', ',').s; //get rid of stray commas
          x = S(x).collapseWhitespace().s; 
          x= S(x).trim().s; 
          if (S(x).isEmpty()) x = "*noData*";
          if (x=="") x = "*noData*";
    x = x.replace(/(\r\n|\n|\r)/gm,""); //removes all 3 types of line break 
        if (!asciiJSON.isAscii(x)) {
          x = jsesc(x); //escapes various language special/accent characters
          };
      return x; 
};


function updateMapWith(csvObj,ctr){
var mappedFields = {};
if (typeof csvObj === 'undefined'){
  //console.log ("UNDEFINED csv object");
  logRejects ("line: " + ctr + " is undefined ");
 }else{
//console.log(csvObj.LegalEntityIdentifier);

var lei = {};
var city = {};
var hqcity = {};
var country = {};
var hqcountry = {};


lei.leiID = csvObj.LegalEntityIdentifier;
lei.regName = csvObj.RegisteredName;
var addrArray = [];
addrArray.push(csvObj.RegisteredAddress1);
addrArray.push(csvObj.RegisteredAddress2);
addrArray.push(csvObj.RegisteredAddress3);
addrArray.push(csvObj.RegisteredAddress4);
addrArray.push(csvObj.RegisteredCity);
addrArray.push(csvObj.RegisteredRegion);
addrArray.push(csvObj.RegisteredPostalCode);
addrArray.push(csvObj.RegisteredCountryCode);

var reducestr = addrArray.join(nL);
reducestr= S(reducestr).replaceAll(nL+nL, nL).s;
lei.address = S(reducestr).replaceAll(nL+nL, nL).s;


var addrArray = [];
addrArray.push(csvObj.HeadquarterAddress1);
addrArray.push(csvObj.HeadquarterAddress2);
addrArray.push(csvObj.HeadquarterAddress3);
addrArray.push(csvObj.HeadquarterAddress4);
addrArray.push(csvObj.HeadquarterCity);
addrArray.push(csvObj.HeadquarterRegion);
addrArray.push(csvObj.HeadquarterPostalCode);
addrArray.push(csvObj.HeadquarterCountryCode);

var reducestr = addrArray.join(nL);
reducestr= S(reducestr).replaceAll(nL+nL, nL).s;
lei.HQaddress = S(reducestr).replaceAll(nL+nL, nL).s;

lei.dateAssigned = csvObj.LEIAssignmentDate;
lei.dateUpdated = csvObj.LEIRecordLastUpdate;
lei.legalForm = csvObj.EntityLegalForm;

//Lei.fields = ['LegalEntityIdentifier', 'RegisteredName', 'RegisteredCity'];
//console.log(csvObj);

var regcityCountry = csvObj.RegisteredCity +'_'+csvObj.RegisteredCountryCode;
regcityCountry=S(regcityCountry).slugify().s;
//var mappedFields = {};
city.cityCountry = regcityCountry;
city.city = csvObj.RegisteredCity;

var HQcityCountry = csvObj.HeadquarterCity +'_'+csvObj.HeadquarterCountryCode;
HQcityCountry=S(HQcityCountry).slugify().s; //gets rid of spaces 

hqcity.cityCountry = HQcityCountry;
hqcity.city = csvObj.HeadquarterCity;

//var mappedFields = {};
country.countryCode = csvObj.RegisteredCountryCode;

//var mappedFields = {};
hqcountry.countryCode = csvObj.HeadquarterCountryCode;

mappedFields.lei=lei;
mappedFields.city=city;
mappedFields.country=country;
mappedFields.hqcity=hqcity;
mappedFields.hqcountry=hqcountry;
console.log(
 // mappedFields
 //merge(mappedFields,lei,city,hqcity,country,hqcountry)
);
}; //end if undefined
return mappedFields;
};


//MAKE THE RELATIONS


function buildRelations(){

co(function *(){
  var a = relateThese(curIDset.leiID,"in_City",curIDset.cityID,{ for: 'test data' });
  var b = relateThese(curIDset.leiID,"in_Country",curIDset.countryID,{ for: 'test data' });
  var c = relateThese(curIDset.hqcityID,"in_Country",curIDset.countryID,{ for: 'test data' });
var d = relateThese(curIDset.cityID,"in_Country",curIDset.countryID,{ for: 'test data' });
var e = relateThese(curIDset.leiID,"HQ_in_City",curIDset.hqcityID,{ for: 'test data' });
var f = relateThese(curIDset.leiID,"HQ_in_Country",curIDset.hqcountryID,{ for: 'test data' });
  var res = yield [a, b, c, d, e, f];
  
  ee.emit("relationsBuilt");
})()


};




function relateThese(fromNode,related,tonode, descriptor) {
fromNode = S(fromNode).toInt();
tonode = S(tonode).toInt();
if ((typeof fromNode != "number") || (typeof tonode != "number")){
    console.log('No relation here ... This is not number');
    console.log ("from is: ", fromNode, " to is: ", tonode);
    if (err) logRejects ("line: " + ctr + "relation prob ");
    return {};
}else{ 

var deferred = Q.defer();
db.relate(fromNode, related, tonode, descriptor, function (error, text) {
    if (error) {
      console.log ("relation error: ", error," for ", curIDset);
        deferred.reject(new Error(error));
    } else {
        deferred.resolve(text);
       // console.log ("relation: ", text);
    }
});

return deferred.promise;

}//end if not a number check
};




/*
function dothis() {

co(function *(){
//Console.log(city.city,city.hqcity,country.country,country.hqcountry);
  var a = saveTheCity(mapped.city,curIDset.cityID);
  var aa = saveTheCity(mapped.hqcity,curIDset.hqcityID);
  var b = saveTheCountry(mapped.country,curIDset.countryID);
  var bb = saveTheCountry(mapped.hqcountry,curIDset.hqcountryID);
  var c = saveTheLei(mapped.lei,curIDset.leiID);
  var res = yield [a,aa, b, c];
//  console.log("updates ok?  ",res,curIDset);
  ee.emit("updateSavesDone");
})()

};
*/

function dothisYY() {

co(function *(){
//Console.log(city.city,city.hqcity,country.country,country.hqcountry);
var c = saveTheNodeWithLabel(mapped.lei,'leiID',['LEI', 'REF_DATA']);
var b = saveTheNodeWithLabel(mapped.country,'countryID',['COUNTRY']);
var bb = saveTheNodeWithLabel(mapped.hqcountry,'hqcountryID',['COUNTRY']);
var a = saveTheNodeWithLabel(mapped.city,'cityID',['CITY']);
var aa = saveTheNodeWithLabel(mapped.hqcity,'hqcityID',['CITY']);
  var res = yield [a,aa, b, c];
  //console.log("updates ok?  ",res,curIDset);
 
})()

};

function dotheseSaves() {

saveTheNodeWithLabel(mapped.lei,'leiID',['LEI', 'REF_DATA']);
saveTheNodeWithLabel(mapped.country,'countryID',['COUNTRY']);
saveTheNodeWithLabel(mapped.hqcountry,'hqcountryID',['COUNTRY']);
saveTheNodeWithLabel(mapped.city,'cityID',['CITY']);
saveTheNodeWithLabel(mapped.hqcity,'hqcityID',['CITY']);

};


function saveTheCityX(themappedfields,targetID) {
var deferred = Q.defer();

//console.log("this db connection happened:  ",db);
var City = model(db, 'cityCountry');
//var themappedfields = {tester:"thetest"};
City.save(themappedfields, function (error, text) {
    if (error) {
        deferred.reject(new Error(error));
    } else {
        deferred.resolve(text);
        targetID=text.id;
        console.log ("city id is: ", targetID);
        //deferred.notify("City done");
    }
});

return deferred.promise;
};

function saveTheCountryX(themappedfields,targetID) {
var deferred = Q.defer();
var Country = model(db, 'country');
//var themappedfields = {tester:"thetest"};
Country.save(themappedfields, function (error, text) {
    if (error) {
        deferred.reject(new Error(error));
    } else {
        deferred.resolve(text);
        targetID=text.id;
        console.log ("country id is: ", targetID);
        //deferred.notify("Country done");
    }
});

return deferred.promise;
};




function saveTheLeiX(mappedFields,targetID) {
var deferred = Q.defer();
var Lei = model(db, 'lei');
//var themappedfields = {tester:"thetest"};
Lei.save(mappedFields.lei, function (error, text) {
    if (error) {
        deferred.reject(new Error(error));
    } else {
        deferred.resolve(text);
        targetID=text.id;
        console.log ("lei id is: ", targetID);
        //deferred.notify("Lei done");
    }
});

return deferred.promise;
};





//EVENTS AND RUN ORDER

var events = require("events");
var EventEmitter = require("events").EventEmitter;

var ee = new EventEmitter();
ee.on("updateSavesDone", function () {
    console.log("saves were updated");
    buildRelations();
});

ee.on("zollMeister", function () {
    console.log("now we are labbeling it ");
    // ee.emit("updateSavesDone");
    //next row
    
});


ee.on("relationsBuilt", function () {
    console.log("nodes and relations built for row",ctr);
    //next row
    prepareRow();
});

ee.on("neoUpdated", function () {
    console.log("neo was updated");
    next = file.next();
    var nextcsvline = next.value;
    currentLine = next.value;
    if (typeof nextcsvline === 'undefined') nextcsvline = "no line";
   // console.info( next.value );
   // dothis(nextcsvline);
   
   updateTogether();
});
   



//setIndicesOnFirstRun();
headerAsArray = getHeader();
prepareRow() ; //GET THE BALL ROLLING








