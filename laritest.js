#!/usr/bin/env nodejs

var LariClient=require(__dirname+'/lariclient.js').LariClient;
var client=new LariClient();
//client.setLariServerUrl('http://localhost:6057');
client.setLariServerUrl('https://lari1.herokuapp.com');
client.setContainerId('child');

//test1();
test2(function(result) {
	test2b(result);
});


function test1(callback) {
	var query={
		processor_name:'ms3.bandpass_filter'
	};
	client.spec(query,{},function(err,spec) {
		if (err) {
			console.error('Error: '+err);
			return;
		}
		console.log (JSON.stringify(spec,null,4));
		if (callback) callback();
	});
}

function test2(callback) {
	var query={
		processor_name:'phret.create_phantom2d',
		package_uri:'https://github.com/magland/ml_phret.git#master',
		inputs:{},
		outputs:{phantom_out:true},
		parameters:{N:100,name:'disk'},
		opts:{}
	};
	client.queueProcess(query,{},function(err,resp) {
		if (err) {
			console.error('Error: '+err);
			return;
		}
		console.log (JSON.stringify(resp,null,4));
		var job_id=resp.job_id;
		do_check();
		function do_check() {
			client.probeProcess(job_id,{},function(err2,resp2) {
				if (err2) {
					console.error('Error: '+err2);
					return;
				}
				console.log (JSON.stringify(resp2,null,4));
				if (resp2.complete) {
					if (callback) callback(resp2.result);
					return;	
				}
				setTimeout(function() {do_check();},1000)
			});
		}
	});
}

function test2b(result) {
	client.getFileContent(result.outputs.phantom_out,{},function(err,resp) {
		if (err) {
			console.error('ERROR: '+err);
			return;
		}
		console.log (resp.content.slice(0,400));
	});
}
