"use strict";

/* see http://chrislarson.me/blog/install-neo4j-graph-database-ubuntu
The database can be cleared out by deleting the folder it is stored in.
cd /var/lib/neo4j/data
sudo rm -rf graph.db/
Restart neo4j
sudo /etc/init.d/neo4j-service restart
to get labels ...
curl http://localhost:7474/db/data/schema/index/


CREATE CONSTRAINT ON (c:COUNTRY) ASSERT c.countryCode IS UNIQUE;
CREATE CONSTRAINT ON (ci:CITY) ASSERT ci.cityCountry IS UNIQUE;
CREATE CONSTRAINT ON (le:LEI) ASSERT le.leiID IS UNIQUE;
*/

//step through a csv file line by line
//each line mapped to cypher query, 
//line reading is paused until query callback triggers nextline event

var co = require('co')
 , request = require('co-request');
var neo4j = require('node-neo4j');
var util = require('util');
var cluster = require('cluster')
  , net = require('net-cluster')
  ;
var neodb = new neo4j('http://localhost:7474');
var LineByLineReader = require('line-by-line');
var jsesc = require('jsesc');
var S = require('string'); //see http://stringjs.com useful string conversions
var Lazy = require('lazy.js');
var asciiJSON = require('ascii-json');
var path = require('path');
var src = 'pleiFull_20140824';
var csvDir = '/home/evengers/Documents/projects/CSVsToLoad';
var theCsv = path.join(csvDir, src +'.csv');

var lr = new LineByLineReader(theCsv);
var events = require('events');
var ev = new events.EventEmitter();

//GLOBALS
var lineCtr=0;
var nL = '\n';
var Cr = '\r';
var firstline = true;
var verbose = false;
var headerAsArray = []; // a global used when each row is prepared
var mapped = {};
var indexesAreNotSet = false; //set true on first run
var startAtLine = 0; //28000 before config change max set this to last line in case job interrupted 
if (indexesAreNotSet) {
       if (verbose)console.log ("Creating indexes and constraints");
       setIndexes(); 
     }else{
       goWithTheFlow();
};

//FLOW SECTION USING EVENTS  
function goWithTheFlow () {
ev.on('getNextLine', function (data, fn) {
    //console.log ("this data just in:" ,data);  
    lr.resume();
});


lr.on('error', function(err){
  //err obj
   console.log(lineCtr, ' ... line had error: ', err);
   // logRejects ( text );  //add to log file
});

lr.on('end', function(line){
  console.log('csv finished! ... lines processed: 30603?', lineCtr);
});

//process line (without newline at end)
lr.on('line', function(line){
  
  lr.pause();  // pause line reader until event triggers resume
  lineCtr++;
  if (firstline) {
         headerAsArray = getHeader(line);
         ev.emit('getNextLine', "optional", function(){});
     }else{
       if (lineCtr > startAtLine){
              console.log ("working on: ",lineCtr);
              prepareRow(line);
          }else{
          //otherwise just get the next line
           ev.emit('getNextLine', "optional", function(){}); 
         }; //end if not at start line
    }; //end if firstline

//  console.log (lineCtr, " : ",line);

}); //end lr.on

//lr.close();  //closes file and issues end event

}; //end gowiththeflow

// END OF FLOW SECTION   



//mapping tidy up and for each of the csv lines

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


function getHeader(csvline) {
    var theHeaderArray = [];
    theHeaderArray = S(csvline).parseCSV(); //copes with embedded commas
      Lazy(theHeaderArray).each(trimThem) // trim each header item
      console.log (theHeaderArray); 
     firstline = false;
return theHeaderArray;
};


function prepareRow(csvline) {
   if (verbose) console.log ("preparing row ... ", lineCtr);
   var rowAsArray = S(csvline).parseCSV(); //copes with embedded commas 
      Lazy(rowAsArray).each(trimThem) // trim & adjust each row item
      
      var resArray = Lazy(headerAsArray).zip(rowAsArray); //uses the header as keys for row
      var csvObj = Lazy(resArray).toObject(); 
     mapped=updateMapWith(csvObj,lineCtr); //does the mapping
     populateStatements(mapped); //*********
};



function updateMapWith(csvObj,ctr){
if (verbose) console.log ("doing the mapping ... ", lineCtr);
var mappedFields = {};
if (typeof csvObj === 'undefined'){
  //console.log ("UNDEFINED csv object");
  logRejects ("line: " + lineCtr + " is undefined ");
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
lei.geotagged="false";
lei.weblink="empty";
lei.doclink="empty";
lei.newslink="empty";
lei.ubdateby="user";
lei.modifieddate=csvObj.LEIRecordLastUpdate;

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



//create and execute Cypher Query ... ask for next line when finished

function populateStatements(inmapped){
if (verbose) console.log ("populating query ... ", lineCtr);
var cityparam = stringsNoKeyQuotes(mapped.city);
var hqcityparam = stringsNoKeyQuotes(mapped.hqcity);
var countryparam = stringsNoKeyQuotes(mapped.country);
var hqcountryparam = stringsNoKeyQuotes(mapped.hqcountry);
var cityparam = stringsNoKeyQuotes(mapped.city);
var leiparam = stringsNoKeyQuotes(mapped.lei);

var theStatements = [
"MERGE (co:COUNTRY "+ countryparam +" )",
"MERGE (ci:CITY "+ cityparam +" )",
"MERGE (hco:COUNTRY "+ hqcountryparam +" )",
"MERGE (hci:CITY "+ hqcityparam +" )",
"CREATE (le:LEI "+ leiparam +")",
"CREATE (le)-[:IN_CITY]->(ci)",
"CREATE (ci)-[:IN_COUNTRY]->(co)", 
"CREATE (le)-[:IN_COUNTRY]->(co)",
"CREATE (le)-[:IN_HQCITY]->(hci)",
"CREATE (hci)-[:IN_HQCOUNTRY]->(hco)",
"CREATE (le)-[:IN_HQCOUNTRY]->(hco)",
"RETURN *",]; 

var query = theStatements.join('\n');

//console.log(query);

queryWithThisString(query);

if (verbose) console.log ("submitting query ... ", lineCtr);

/*
neodb.cypherQuery(query, function (err, result){
  if (err) { 
      console.log ("got this query error:  ",err);    
      }else{ 
         if (verbose) console.log ("got the callback ... ", lineCtr);
        //callback finished, query done, so get next line
  ev.emit('getNextLine', "optional send something on event", function(){});

    }//end if
});

*/

};








// indexes



function setOneIndex(thelabel, theproperty){


//delete first ... not sure if necessary but indexes seem to get screwed sometimes
/*
neodb.deleteLabelIndex(thelabel, function (err, result){
      if (err) { 
      console.log ("got this error deleting index:  ",err);    
      }else{ 
        console.log ("index delete result:  ", result );   
        //try adding constraint too
     
    }//end if
});

neodb.createUniquenessContstraint(thelabel, theproperty, function (err, result){
      if (err) { 
      console.log ("got this error setting constraint:  ",err);    
      }else{ 
        console.log ("set constraint result:  ", result );   
    
    }//end if
});
*/
neodb.insertLabelIndex(thelabel, theproperty, function (err, result){
      if (err) { 
      console.log ("got this error inserting index:  ",err);    
      }else{ 
        console.log ("index insert result:  ", result );        
    }//end if
}); 

};

function setIndexes(){

//NEED TO RE-work this  seems to fail set in wrong order ?
// only use constraints .. no index??
//setOneIndex('IN_HQCOUNTRY', 'cityCountry');
//setOneIndex('COUNTRY', 'countryCode');
//setOneIndex('CITY', 'cityCountry');
//setOneIndex('LEI', 'leiID');

var theStatements = [
//"DROP INDEX ON :COUNTRY(countryCode);",
//"DROP INDEX ON :CITY(cityCountry);",
//"DROP INDEX ON :LEI(leiID);",
"CREATE INDEX ON :COUNTRY(countryCode)",
"CREATE INDEX ON :CITY(cityCountry)",
"CREATE INDEX ON :LEI(leiID)",
]; 

var theStatements = [
//"DROP INDEX ON :COUNTRY(countryCode);",
//"DROP INDEX ON :CITY(cityCountry);",
//"DROP INDEX ON :LEI(leiID);",
"CREATE CONSTRAINT ON (c:COUNTRY) ASSERT c.countryCode IS UNIQUE;",
"CREATE CONSTRAINT ON (ci:CITY) ASSERT ci.cityCountry IS UNIQUE;",
"CREATE CONSTRAINT ON (le:LEI) ASSERT le.leiID IS UNIQUE;",
]; 

var query = theStatements.join('\n');

//console.log(query);
queryWithThisString(query);


indexesAreNotSet = false;
};



function WORKSqueryWithThisString(astr) {

if (verbose) console.log ("using querystr: ", astr);



if (astr == "test") {
      var proto = [
                   "match n",
                   "with n",                
                   "return count(n)",
                   ];
     strForBody = proto.join('\n');
     console.log("testing with ... ", strForBody);
}

var bodyAsObj = {"query": astr};
var bodyAsStr = JSON.stringify(bodyAsObj);

//theurl = 'http://localhost:7474/db/data/index/node/'+ thelabel + '?uniqueness=get_or_create';
 // uri: 'http://0.0.0.0:7474/db/data/cypher',


var feedback = co(function *(){

 var theurl = 'http://0.0.0.0:7474/db/data/cypher';
  var result = yield request({
    headers: {"Content-Type" : "application/json"},
    uri: theurl,
    method: 'POST',
    body: bodyAsStr
  });
  
  var response = result;
  var responsebody = result.body;
//  console.log("got this response ... ", response);
var arespObj = JSON.parse(responsebody);
var somefeedback = arespObj.message;
 console.log("got this body back ... ", somefeedback);

//return somefeedback;  

if(!indexesAreNotSet) ev.emit('getNextLine', "optional send something on event", function(){});

})();

console.log ("??" , feedback);

};



function queryWithThisString(astr) {

if (verbose) console.log ("using querystr: ", astr);



if (astr == "test") {
      var proto = [
                   "match n",
                   "with n",                
                   "return count(n)",
                   ];
     strForBody = proto.join('\n');
     console.log("testing with ... ", strForBody);
}

var bodyAsObj = {"query": astr};
var bodyAsStr = JSON.stringify(bodyAsObj);

//theurl = 'http://localhost:7474/db/data/index/node/'+ thelabel + '?uniqueness=get_or_create';
 // uri: 'http://0.0.0.0:7474/db/data/cypher',


function *askMe(){

 var theurl = 'http://0.0.0.0:7474/db/data/cypher';
  var result = yield request({
    headers: {"Content-Type" : "application/json"},
    uri: theurl,
    method: 'POST',
    body: bodyAsStr
  });
  
  var response = result;
  var responsebody = result.body;
//  console.log("got this response ... ", response);
var arespObj = JSON.parse(responsebody);
var somefeedback = arespObj.message;
 

return somefeedback;  

};

// Node 0 already exists with label COUNTRY and property "countryCode"=[US]
co(function *(){
 var results = yield askMe();
 if (results) {
      console.log("got this back ... ", results);
      var theId = S(results).between('Node', 'already').s;
      theId = S(theId).trim().s;
      console.log ("node: ", theId);
   }
if(!indexesAreNotSet) ev.emit('getNextLine', "optional send something on event", function(){});


})();



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


