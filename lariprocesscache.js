exports.LariProcessCache=LariProcessCache;

var sha1=require(__dirname+'/sha1.js');

function LariProcessCache() {
	var that=this;

	this.setCachedResponse=function(query,resp,callback) {setCachedResponse(query,resp,callback);};
	this.getCachedResponse=function(query,callback) {getCachedResponse(query,callback);};

	var m_cached_responses_by_code={};

	function setCachedResponse(query,resp,callback) {
		var query_opts=query.opts||{};
		if (query_opts.force_run) {
			callback();
			return;
		}
		var code=compute_process_code(query);
		var resp0=clone(resp);
		resp0.latest_console_output='cached ('+(new Date())+')';
		m_cached_responses_by_code[code]=resp0;
		callback();
	}

	function getCachedResponse(query,callback) {
		var query_opts=query.opts||{};
		if (query_opts.force_run) {
			callback(null);
			return;
		}
		var code=compute_process_code(query);
		if (!(code in m_cached_responses_by_code)) {
			callback(null);
			return;
		}
		callback(clone(m_cached_responses_by_code[code]));
	}

	function compute_process_code(query) {
		var obj={};
		obj.processor_name=query.processor_name;
		obj.processor_version=query.processor_version;
		obj.inputs=query.inputs;
		obj.outputs=query.outputs;
		obj.parameters=query.parameters;
		obj.package_uri=(query.opts||{}).package_uri||'';
		var code=sha1(JSON.stringify(obj));
		return code;
	}

	function clone(obj) {
		return JSON.parse(JSON.stringify(obj));
	}
}
