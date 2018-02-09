exports.LariJobManager=LariJobManager;
exports.LariProcessorJob=LariProcessorJob;
exports.execute_and_read_stdout=execute_and_read_stdout;

function LariJobManager() {
	var that=this;

	this.addJob=function(job_id,J) {m_jobs[job_id]=J;};
	this.job=function(job_id) {return job(job_id);};
	this.removeJob=function(job_id) {removeJob(job_id);};

	var m_jobs={};

	function housekeeping() {
		//cleanup here
		setTimeout(housekeeping,10000);	
	}
	//setTimeout(housekeeping,10000);	
	function removeJob(job_id) {
		delete m_jobs[job_id];
	}
	function job(job_id) {
		if (job_id in m_jobs) {
			return m_jobs[job_id];
		}
		else return null;
	}
}

function LariProcessorJob() {
	var that=this;

	this.start=function(processor_name,inputs,outputs,parameters,resources,opts,callback) {start(processor_name,inputs,outputs,parameters,resources,opts,callback);};
	this.keepAlive=function() {m_alive_timer=new Date();};
	this.cancel=function(callback) {cancel(callback);};
	this.isComplete=function() {return m_is_complete;};
	this.result=function() {return m_result;};
	this.elapsedSinceKeepAlive=function() {return (new Date())-m_alive_timer;};

	this.outputFilesStillValid=function() {return outputFilesStillValid();};
	this.takeLatestConsoleOutput=function() {return takeLatestConsoleOutput();};

	var m_result=null;
	var m_alive_timer=new Date();
	var m_is_complete=false;
	var m_process_object=null;
	var m_output_file_stats={};
	var m_latest_console_output='';

	var request_fname,response_fname,mpreq;
	function start(processor_name,inputs,outputs,parameters,resources,opts,callback) {
		request_fname=make_tmp_json_file(process.env.DATA_DIRECTORY);
		response_fname=make_tmp_json_file(process.env.DATA_DIRECTORY);
		mpreq={
			action:'queue_process',
			processor_name:processor_name,
			inputs:inputs,
			outputs:outputs,
			parameters:parameters,
			resources:resources,
			package_uri:opts.package_uri||''
		};
		if (!lari_write_text_file(request_fname,JSON.stringify(mpreq))) {
			callback('Problem writing mpreq to file');
			return;
		}
		callback(null);
		setTimeout(start2,10);
	}
	function start2() {
		setTimeout(housekeeping,1000);
		m_process_object=execute_and_read_stdout('mproc',['handle-request','--prvbucket_path='+process.env.DATA_DIRECTORY,request_fname,response_fname],{on_stdout:on_stdout},function(txt) {
			remove_file(request_fname);
			if (!require('fs').existsSync(response_fname)) {
				m_is_complete=true;
				m_result={success:false,error:'Response file does not exist: '+response_fname};
				return;
			}
			var json_response=read_json_file(response_fname);
			remove_file(response_fname);
			if (json_response) {
				m_is_complete=true;
				m_result=json_response;
				m_output_file_stats=compute_output_file_stats(m_result.outputs);
			}
			else {
				m_is_complete=true;
				m_result={success:false,error:'unable to parse json in response file'};
			}
		});
		function on_stdout(txt) {
			m_latest_console_output+=txt;
		}
	}
	function takeLatestConsoleOutput() {
		var ret=m_latest_console_output;
		m_latest_console_output='';
		return ret;
	}
	function cancel(callback) {
		if (m_is_complete) {
			if (callback) callback(null); //already complete
			return;
		}
		if (m_process_object) {
			console.log ('Canceling process: '+m_process_object.pid);
			m_process_object.stdout.pause();
			m_process_object.kill('SIGTERM');
			m_is_complete=true;
			m_result={success:false,error:'Process canceled'};
			if (callback) callback(null);
		}
		else {
			if (callback) callback('m_process_object is null.');
		}
	}
	function housekeeping() {
		if (m_is_complete) return;
		var timeout=20000;
		var elapsed_since_keep_alive=that.elapsedSinceKeepAlive();
		if (elapsed_since_keep_alive>timeout) {
			console.log ('Canceling process due to keep-alive timeout');
			cancel();
		}
		else {
			setTimeout(housekeeping,1000);
		}
	}
	function make_tmp_json_file(data_directory) {
		return data_directory+'/tmp.'+make_random_id(10)+'.json';
	}
	function make_random_id(len) {
	    var text = "";
	    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

	    for( var i=0; i < len; i++ )
	        text += possible.charAt(Math.floor(Math.random() * possible.length));

	    return text;
	}
	function compute_output_file_stats(outputs) {
		var stats={};
		for (var key in outputs) {
			stats[key]=compute_output_file_stat(outputs[key].original_path);
		}
		return stats;
	}
	function compute_output_file_stat(path) {
		try {
			var ss=require('fs').statSync(path);
			return {
				exists:require('fs').existsSync(path),
				size:ss.size,
				last_modified:(ss.mtime+'') //make it a string
			};
		}	
		catch(err) {
			return {};
		}
	}
	function outputFilesStillValid() {
		var outputs0=(m_result||{}).outputs||{};
		var stats0=m_output_file_stats||{};
		var stats1=compute_output_file_stats(outputs0);
		for (var key in stats0) {
			var stat0=stats0[key]||{};
			var stat1=stats1[key]||{};
			if (!stat1.exists) {
				return false;
			}
			if (stat1.size!=stat0.size) {
				return false;
			}
			if (stat1.last_modified!=stat0.last_modified) {
				return false;
			}
		}
		return true;
	}
}

function lari_write_text_file(fname,txt) {
	try {
		require('fs').writeFileSync(fname,txt,'utf8');
		return true;
	}
	catch(e) {
		console.error('Problem writing file: '+fname);
		return false;
	}	
}

function execute_and_read_stdout(exe,args,opts,callback) {
	var P;
	try {
		P=require('child_process').spawn(exe,args);
	}
	catch(err) {
		console.log (err);
		callback("Problem launching: "+exe+" "+args.join(" "));
		return;
	}
	var txt='';
	P.stdout.on('data',function(chunk) {
		txt+=chunk;
		if (opts.on_stdout) {
			opts.on_stdout(chunk);
		}
	});
	P.on('close',function(code) {
		callback(null,txt);
	});
	return P;
}

function remove_file(fname) {
	try {
		require('fs').unlinkSync(fname);
		return true;
	}
	catch(err) {
		return false;
	}
}

function read_text_file(fname) {
	try {
		return require('fs').readFileSync(fname,'utf8');
	}
	catch(err) {
		return '';
	}
}

function read_json_file(fname) {
	try {
		return JSON.parse(read_text_file(fname));
	}
	catch(err) {
		return '';
	}	
}