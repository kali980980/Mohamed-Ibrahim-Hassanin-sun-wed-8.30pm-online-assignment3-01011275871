const fs = require("node:fs");

fs.readFile("./input.txt", "utf-8",(err,data) =>{
    if(err){
      console.log(err);
      return;
    }
    console.log("file content", data);

});