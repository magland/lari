#!/usr/bin/env nodejs

// Set the following environment variables:
//   CONTAINER_ID (required)

// If you want this to be a processing server (must have mountainlab installed)
//   DATA_DIRECTORY (use * for default)

// If you want to listen
//   LISTEN_PORT (e.g., 6057, or * for default)

// To connect to a parent lari server
//   PARENT_LARI_URL (optional)

// Not used yet
//   DOCSTOR_URL (optional)
//   CONFIG_DOC (optional)

require('dotenv').config(__dirname+'/.env');

if (!process.env.CONTAINER_ID) {
	console.warn('Environment variable not set: CONTAINER_ID.');
	return;
}

if (process.env.DATA_DIRECTORY) {
	if (process.env.DATA_DIRECTORY=='*') {
		process.env.DATA_DIRECTORY=get_default_data_directory();
	}
	mkdir_if_needed(process.env.DATA_DIRECTORY);
	console.log ('Using data directory: '+process.env.DATA_DIRECTORY);
}

var LariJobManager=require(__dirname+'/larijobmanager.js').LariJobManager;
var LariProcessorJob=require(__dirname+'/larijobmanager.js').LariProcessorJob;
var execute_and_read_stdout=require(__dirname+'/larijobmanager.js').execute_and_read_stdout;
var JM=new LariJobManager();

var LariContainerManager=require(__dirname+'/laricontainermanager.js').LariContainerManager;
var container_manager=new LariContainerManager();

var lari_http_post_json=require(__dirname+'/lariclient.js').lari_http_post_json;

var GoogleAuth = require('google-auth-library');
var jwt = require('jsonwebtoken');
DocStorClient = require(__dirname+'/docstorclient.js').DocStorClient;

var DOCSTOR_URL=process.env.DOCSTOR_URL||'';
var CONFIG_DOC=process.env.CONFIG_DOC||'';

if (CONFIG_DOC) {
	if (!DOCSTOR_URL) {
		console.error('Missing environment variable: DOCSTOR_URL');
		return;
	}
}


var express = require('express');

if (process.env.LISTEN_PORT=='*') {
	process.env.LISTEN_PORT=process.env.PORT||6057;
}

if (process.env.LISTEN_PORT) {
	var app = express();

	app.set('port', (process.env.LISTEN_PORT));

	var lari_config=null;
	var last_update_lari_config=0;
	setTimeout(function() {
		do_update_lari_config();
	},100);


	app.use(function(req,resp,next) {
		var url_parts = require('url').parse(req.url,true);
		var host=url_parts.host;
		var path=url_parts.pathname;
		var query=url_parts.query;
		if (path=='/api/spec') {
			handle_api('spec',req,resp);
		}
		else if (path=='/api/queue-process') {
			handle_api('queue-process',req,resp);
		}
		else if (path=='/api/probe-process') {
			handle_api('probe-process',req,resp);
		}
		else if (path=='/api/cancel-process') {
			handle_api('cancel-process',req,resp);
		}
		else if (path=='/api/get-file-content') {
			handle_api('get-file-content',req,resp);
		}
		else if (path=='/api/poll-from-container') {
			handle_api('poll-from-container',req,resp);
		}
		else if (path=='/api/responses-from-container') {
			handle_api('responses-from-container',req,resp);
		}
		else {
			next();
		}
	});

	app.listen(app.get('port'), function() {
	  console.log ('lari is running on port', app.get('port'));
	});

	function handle_api(cmd,REQ,RESP) {
		var url_parts = require('url').parse(REQ.url,true);
		var host=url_parts.host;
		var path=url_parts.pathname;
		var query=url_parts.query;

		if (REQ.method == 'OPTIONS') {
			var headers = {};
			
			//allow cross-domain requests
			/// TODO refine this
			
			headers["Access-Control-Allow-Origin"] = "*";
			headers["Access-Control-Allow-Methods"] = "POST, GET, PUT, DELETE, OPTIONS";
			headers["Access-Control-Allow-Credentials"] = false;
			headers["Access-Control-Max-Age"] = '86400'; // 24 hours
			headers["Access-Control-Allow-Headers"] = "X-Requested-With, X-HTTP-Method-Override, Content-Type, Accept, Authorization";
			RESP.writeHead(200, headers);
			RESP.end();
		}
		else {
			if (REQ.method=='GET') {
				handle_api_2(cmd,query,create_closer(REQ),function(resp) {
					send_json_response(RESP,resp);
				});
			}
			else if (REQ.method=='POST') {
				receive_json_post(REQ,function(post_query) {
					if (!post_query) {
						var err='Problem parsing json from post';
						console.log (err);
						send_json_response({success:false,error:err});
						return;
					}
					handle_api_2(cmd,post_query,create_closer(REQ),function(resp) {
						send_json_response(RESP,resp);
					});
				});
			}
			else {
				send_json_response({success:false,error:'Unsupported request method: '+REQ.method});
			}
		}
	}
}

if (process.env.PARENT_LARI_URL) {
	console.log ('Connecting to parent: '+process.env.PARENT_LARI_URL);
	var container_client=new ContainerClient();
	container_client.setLariUrl(process.env.PARENT_LARI_URL);
	container_client.setContainerId(process.env.CONTAINER_ID);
	container_client.setRequestHandler(function(cmd,query,callback) {
		console.log ('Handling api request in container: '+cmd);
		handle_api_2(cmd,query,create_closer(null),function(resp) {
			callback(resp);
		});
	});
	container_client.start();
}

function handle_api_2(cmd,query,closer,callback) {
	if (cmd=='poll-from-container') {
		var debug_code=lari_make_random_id(5);
		container_manager.handlePollFromContainer(query,closer,function(resp) {
			callback(resp);
		});
		return;
	}
	if (cmd=='responses-from-container') {
		container_manager.handleResponsesFromContainer(query,closer,function(resp) {
			callback(resp);
		});
		return;
	}

	if (query.container_id!=process.env.CONTAINER_ID) {
		container_manager.handleApiRequest(cmd,query,closer,function(resp) {
			callback(resp);
		});
		return;
	}
	if (cmd=='spec') {
		spec(query,closer,function(err,resp) {
			if (err) {
				callback({success:false,error:err});
			}
			else {
				callback(resp);
			}
		});
	}
	else if (cmd=='queue-process') {
		queue_process(query,closer,function(err,resp) {
			if (err) {
				callback({success:false,error:err});
			}
			else {
				callback(resp);
			}
		});
	}
	else if (cmd=='probe-process') {
		probe_job(query,function(err,resp) {
			if (err) {
				callback({success:false,error:err});
			}
			else {
				callback(resp);
			}
		});
	}
	else if (cmd=='cancel-process') {
		cancel_job(query,function(err,resp) {
			if (err) {
				callback({success:false,error:err});
			}
			else {
				callback(resp);
			}
		});
	}
	else if (cmd=='get-file-content') {
		get_file_content(query,closer,function(err,resp) {
			if (err) {
				callback({success:false,error:err});
			}
			else {
				callback(resp);
			}
		});
	}
	else {
		callback({success:false,error:'Unsupported command: '+cmd});	
	}

	function spec(query,closer,callback) {
		if (!query.processor_name) {
			callback('Missing query parameter: processor_name');
			return;
		}
		var exe='mp-spec';
		var args=[query.processor_name];
		if (query.package_uri) {
			args.push('--_package_uri='+query.package_uri);
		}
		var opts={closer:closer};
		console.log ('RUNNING: '+exe+' '+args.join(' '));
		var pp=execute_and_read_stdout(exe,args,opts,function(err,stdout) {
			if (err) {
				console.error('Error in spec: '+err);
				callback(err);
				return;
			}
			console.log ('Output of process: ['+stdout.length+' bytes]');
			var obj=try_parse_json(stdout);
			if (!obj) {
				console.error('ERROR PARSING for: '+exe);
				console.error(stdout);
				callback('Error parsing JSON for command: '+exe);
				return;
			}
			callback(null,{success:true,spec:obj});
		});
		closer.on('close',function() {
			console.log ('Canceling spec process.');
			pp.stdout.pause();
			pp.kill('SIGTERM');
		});
	}
	function get_file_content(query,closer,callback) {
		if (!process.env.DATA_DIRECTORY) {
			callback('Environment variable not set: DATA_DIRECTORY');
			return;
		}
		if ((!query.checksum)||(!('size' in query))||(!('fcs' in query))) {
			callback("Invalid query.");	
			return;
		}
		var pp=execute_and_read_stdout('prv-locate',['--search_path='+process.env.DATA_DIRECTORY,'--checksum='+query.checksum,'--size='+query.size,'--fcs='+(query.fcs||'')],{},function(err,path) {
			path=path.trim();
			var size0=Number(query.size);
			if (size0>1024*1024) {
				callback('Cannot get file content for file of size '+size0);
				return;
			}
			if (err) {
				callback('Error in prv-locate: '+err);
				return;
			}
			if (!require('fs').existsSync(path)) {
				callback('Unable to find file: '+path);
				return;
			}
			var content=read_text_file(path);
			if (!content) {
				callback('Unable to read text file, or file is empty: '+path);
				return;
			}
			callback(null,{success:true,content:content});
		});
		closer.on('close',function() {
			console.log ('Canceling prv-locate process.');
			pp.stdout.pause();
			pp.kill('SIGTERM');
		});
	}
	function read_text_file(fname) {
		try {
			return require('fs').readFileSync(fname,'utf8');
		}
		catch(err) {
			return '';
		}
	}
	function queue_process(query,closer,callback) {
		var processor_name=query.processor_name||'';
		var inputs=query.inputs||{};
		var outputs=query.outputs||{};
		var parameters=query.parameters||{};
		var resources=query.resources||{};
		var opts=query.opts||{};
		if ('process_id' in query) {
			callback('process_id parameter is not allowed in queue-process.');
			return;
		}
		var job_id=lari_make_random_id(10);
		var Jnew=new LariProcessorJob();
		Jnew.start(processor_name,inputs,outputs,parameters,resources,opts,function(err,tmp) {
			if (err) {
				callback(err);
				return;
			}
			JM.addJob(job_id,Jnew);
			var check_timer=new Date();
			if (!('wait_msec' in query))
				query.wait_msec=100;
			check_it();
			function check_it() {
				var elapsed=(new Date()) -check_timer;
				if ((elapsed>=Number(query.wait_msec))||(Jnew.isComplete())) {
					make_response_for_job(job_id,Jnew,callback);
					return;
				}
				else {
					setTimeout(check_it,10);
				}
			}
		});	
		closer.on('close',function() {
			console.log ('Canceling process.');
			Jnew.cancel();
		});
	}
	function probe_job(query,callback) {
		var job_id=query.job_id||'';
		var J=JM.job(job_id);
		if (!J) {
			callback('Job with id not found: '+job_id);
			return;
		}
		J.keepAlive();
		make_response_for_job(job_id,J,callback);
	}
	function cancel_job(query,callback) {
		var job_id=query.job_id||'';
		var J=JM.job(job_id);
		if (!J) {
			callback('Job with id not found: '+job_id);
			return;
		}
		J.cancel(callback);
	}
	function make_response_for_job(job_id,J,callback) {
		var resp={success:true};
		resp.job_id=job_id;
		resp.complete=J.isComplete();
		if (J.isComplete())
			resp.result=J.result();
		resp.latest_console_output=J.takeLatestConsoleOutput();
		callback(null,resp);
	}
}

function ContainerClient() {
	this.setContainerId=function(id) {m_container_id=id;};
	this.setLariUrl=function(url) {m_url=url;};
	this.start=function() {start();};
	this.setRequestHandler=function(handler) {m_request_handler=handler;};

	var m_container_id='';
	var m_url='';
	var m_running=false;
	var m_active_polls={};
	var m_request_handler=function(cmd,query,callback) {
		callback({success:false,error:'no request handler has been set.'});
	};

	function start() {
		m_running=true;
	}

	function add_poll() {
		if (!m_container_id) {
			console.error('Container id not set in ContainerClient.');
			return;
		}
		var poll={
			timestamp:new Date()
		};
		var poll_id=lari_make_random_id(10);
		m_active_polls[poll_id]=poll;

		var url=m_url+'/api/poll-from-container';
		console.log ('Making poll request to: '+url);
		lari_http_post_json(url,{container_id:m_container_id},{},function(err,resp) {
			if (err) {
				console.error('Error making poll request to lari server: '+err);
				delete m_active_polls[poll_id];
				return;
			}
			if (!resp.success) {
				console.error('Error in poll request to lari server: '+resp.error);
				delete m_active_polls[poll_id];
				return;
			}
			if ('requests' in resp) {
				for (var i in resp.requests) {
					handle_request_from_server(resp.requests[i]);
				}
			}
			delete m_active_polls[poll_id];
			add_poll();
		});
	}

	function handle_request_from_server(req) {
		var request_id=req.request_id;
		if (!request_id) {
			console.warn('No request id in request from server!');
			return;
		}
		m_request_handler(req.cmd,req.query,function(resp) {
			send_response_to_request_from_server(request_id,resp);
		});
	}

	function send_response_to_request_from_server(request_id,resp) {
		var responses=[];
		responses.push({
			request_id:request_id,
			response:resp
		});
		var url=m_url+'/api/responses-from-container';
		lari_http_post_json(url,{responses:responses,container_id:m_container_id},{},function(err0,resp0) {
			if (err0) {
				console.error('Error sending responses to lari server: '+err0);
				return;
			}
			if (!resp0.success) {
				console.error('Error in sending responses to lari server: '+resp.error);
				return;
			}
		});	
	}

	function housekeeping() {
		if (m_running) {
			var num_active_polls=0;
			for (var id in m_active_polls) {
				num_active_polls++;
			}
			if (num_active_polls<1) {
				add_poll();
			}
		}
		setTimeout(function() {
			housekeeping();
		},1000);
	}
	setTimeout(function() {
		housekeeping();
	},500);
}

/*
function decode_jwt(token) {
	var list=token.split('.');
	var str0=list[1]||'';
	return JSON.parse(require('atob')(str0));
}

function create_authorization_jwt(authorization,options) {
	var secret=process.env.KBUCKET_SECRET||'';
	if (!secret) return null;
	return jwt.sign(authorization,secret,options||{});
}
*/

function send_json_response(RESP,obj) {
	RESP.writeHead(200, {"Access-Control-Allow-Origin":"*", "Content-Type":"application/json"});
	RESP.end(JSON.stringify(obj));
}

function receive_json_post(REQ,callback_in) {
	var callback=callback_in;
	var body='';
	REQ.on('data',function(data) {
		body+=data;
	});
	REQ.on('error',function() {
		if (callback) callback(null);
		callback=null;
	})
	
	REQ.on('end',function() {
		var obj;
		try {
			var obj=JSON.parse(body);
		}
		catch (e) {
			if (callback) callback(null);
			callback=null;
			return;
		}
		if (callback) callback(obj);
		callback=null;
	});
}

function do_update_lari_config_if_needed(callback) {
	var elapsed=(new Date())-last_update_lari_config;
	if (elapsed>5000) {
		do_update_lari_config(callback);
	}
	else {
		callback();
	}
}

function do_update_lari_config(callback) {
	do_update_lari_config_2(function(err,obj) {
		last_update_lari_config=new Date();
		if (err) {
			console.error(err);
			if (callback) callback();
		}
		else {
			if (JSON.stringify(obj)!=JSON.stringify(lari_config)) {
				console.info('Setting new lari_config.');
				console.info(JSON.stringify(obj));
				lari_config=obj;
			}
			if (callback) callback();
		}
	});
}

function do_update_lari_config_2(callback) {
	if (!CONFIG_DOC) {
		callback(null,{});
		return;
	}
	var tmp=CONFIG_DOC.split(':');
	if (tmp.length!=2) {
		console.error('Error in CONFIG_DOC: '+CONFIG_DOC);
		process.exit(-1);
	}
	var owner=tmp[0];
	var title=tmp[1];

	var CC=new DocStorClient();
	CC.setDocStorUrl(DOCSTOR_URL);
	CC.findDocuments({owned_by:owner,and_shared_with:'[public]',filter:{"attributes.title":title}},function(err2,tmp) {
		if (err2) {
			callback('Error getting document: '+title+'('+owner+'): '+err2);
			return;
		}
		if (tmp.length===0) {
			callback('Unable to find document: '+title+' ('+owner+')');
			return;	
		}
		if (tmp.length>1) {
			callback('More than one document found with title: '+title);
			return;	
		}
		CC.getDocument(tmp[0]._id,{include_content:true},function(err3,tmp) {
			if (err3) {
				callback('Error retreiving config content of document '+title+': '+err3);
				return;
			}
			var obj=try_parse_json(tmp.content);
			if (!obj) {
				callback('Error parsing json content of config for document '+title);
				return;
			}
			callback(null,obj);
		});
	});
}

function try_parse_json(json) {
	try {
		return JSON.parse(json);
	}
	catch(err) {
		return null;
	}
}


function lari_make_random_id(len) {
    var text = "";
    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    for( var i=0; i < len; i++ )
        text += possible.charAt(Math.floor(Math.random() * possible.length));

    return text;
}

function mkdir_if_needed(path) {
	try {
		require('fs').mkdirSync(path);
	}
	catch(err) {

	}
}

function get_default_data_directory() {
	var conf=get_ml_config();
	if (!conf) {
		throw new Error('Unable to get mlconfig');
	}
	var tmpdir=(conf.general||{}).temporary_path||null;
	return tmpdir+'/lari';
}

function get_ml_config() {
	var r = require('child_process').execSync('mlconfig print');
	var str=r.toString();
	var obj=try_parse_json(str.trim());
	if (!obj) {
		console.error('Error parsing json in output of mlconfig');
		return null;
	}
	return obj;
}

function create_closer(REQ) {
	return REQ||{on:function(str,handler) {}};
}