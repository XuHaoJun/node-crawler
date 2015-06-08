'use strict';

var path = require('path');
var util = require('util');
var EventEmitter = require('events').EventEmitter;

var request = require('request');
var _ = require('lodash');
var jschardet = require('jschardet');
var cheerio = require('cheerio');
var zlib = require('zlib');
var fs = require('fs');
var Pool = require('generic-pool').Pool;

// Fallback on iconv-lite if we didn't succeed compiling iconv
// https://github.com/sylvinus/node-crawler/pull/29
var iconv, iconvLite;
try {
  iconv = require('iconv').Iconv;
} catch (e) {}

if (!iconv) {
  iconvLite = require('iconv-lite');
}

function useCache (options) {
  return (
    options.uri &&
      (options.cache || options.skipDuplicates) &&
      (options.method === 'GET' || options.method === 'HEAD'));
}

function checkJQueryNaming (options) {
  if ('jquery' in options) {
    options.jQuery = options.jquery;
    delete options.jquery;
  }
  return options;
}

function readJqueryUrl (url, callback) {
  if (url.match(/^(file\:\/\/|\w+\:|\/)/)) {
    fs.readFile(url.replace(/^file\:\/\//,''),'utf-8', function(err,jq) {
      callback(err, jq);
    });
  } else {
    callback(null, url);
  }
}

function Crawler (options) {
  var self = this;
  self.init(options);
}
// augment the prototype for node events using util.inherits
util.inherits(Crawler, EventEmitter);

function emptyFunc() {
}

function handlePoolCreate(callback) {
  callback(1);
}

function handlePoolDrain() {
  if (this.options.onDrain) {
    this.options.onDrain();
  }
  this.pool.destroyAllNow();
};

Crawler.prototype.init = function init (options) {
  var defaultOptions = {
    autoWindowClose:    true,
    cache:              false,
    forceUTF8:          false,
    incomingEncoding:   null, //TODO remove or optimize
    jQuery:             true,
    maxConnections:     10,
    method:             'GET',
    onDrain:            false,
    priority:           5,
    priorityRange:      10,
    rateLimits:         0,
    referer:            false,
    retries:            3,
    retryTimeout:       10000,
    skipDuplicates:     false
  };

  //return defaultOptions with overriden properties from options.
  this.options = _.extend(defaultOptions, options);

  // you can use jquery or jQuery
  this.options = checkJQueryNaming(this.options);

  // if using rateLimits we want to use only one connection with delay in between requests
  if (this.options.rateLimits !== 0) {
    this.options.maxConnections = 1;
  }

  // Don't make these options persist to individual queries
  this.globalOnlyOptions = ['maxConnections', 'priorityRange', 'onDrain'];

  //Setup a worker pool w/ https://github.com/coopernurse/node-pool
  this.pool = Pool({
    name         : 'crawler',
    max          : this.options.maxConnections,
    priorityRange: this.options.priorityRange,
    create       : handlePoolCreate,
    destroy      : emptyFunc
  });

  this.plannedQueueCallsCount = 0;
  this.queueItemSize = 0;

  this.cache = {};

  this.on('pool:release', this._release.bind(this));

  this.on('pool:drain', handlePoolDrain.bind(this));
};

Crawler.prototype._release = function _release (options) {
  this.queueItemSize--;
  if (options._poolReference) {
    this.pool.release(options._poolReference);
  }

  // Pool stats are behaving weird - have to implement our own counter
  if (this.queueItemSize + this.plannedQueueCallsCount === 0) {
    this.emit('pool:drain');
  }
};

Crawler.prototype._inject = function _inject (response, options, callback) {
  var $;

  if (options.jQuery === 'cheerio' || options.jQuery.name === 'cheerio' || options.jQuery === true) {
    var defaultCheerioOptions = {
      normalizeWhitespace: false,
      xmlMode: false,
      decodeEntities: true
    };
    var cheerioOptions = options.jQuery.options || defaultCheerioOptions;
    $ = cheerio.load(response.body, cheerioOptions);

    callback(null, $);
  }

  else if (options.jQuery.jsdom) {
    var jsdom = options.jQuery.jsdom;
    var scriptLocation = path.resolve(__dirname, '../vendor/jquery-2.1.1.min.js');

    //Use promises
    readJqueryUrl(scriptLocation, function(err, jquery) {
      try {
        jsdom.env({
          url: options.uri,
          html: response.body,
          src: [jquery],
          done: function (errors, window) {
            $ = window.jQuery;
            callback(errors, $);

            try {
              window.close();
              window = null;
            } catch (err) {
              console.log('Couldn\'t window.close : ' + err);
            }

          }
        });
      } catch (e) {
        options.callback(e);
        this.emit('pool:release', options);
      }
    }.bind(this));
  }
  // Jquery is set to false are not set
  else {
    callback(null);
  }
};

function _handleRemove() {
  this.queue(options.slice(1));
}

Crawler.prototype.queue = function queue (options) {
  // Did you get a single object or string? Make it compatible.
  options = _.isString(options) || _.isPlainObject(options) ? [ options ] : options;
  if (options !== undefined && options.length == 1) {
    this._pushToQueue(
      _.isString(options[0]) ? { uri: options[0] } : options[0]
    );
    // Did you get multiple requests? Queue the URLs.
  } else if (options !== undefined) {
    this.queue(
      _.isString(options[0]) ? { uri: options[0] } : options[0]
    );
    //call with setTimeout to avoid exceed node stack limit
    setTimeout(_handleRemove.bind(this), 0);

  }
};

function _handlePoolAcquire(options, error, poolReference) {
  options._poolReference = poolReference;

  // this is and operation error
  if (error) {
    console.error('pool acquire error:',error);
    options.callback(error);
    return;
  }

  //Static HTML was given, skip request
  if (options.html) {
    this._onContent(null, options, {body:options.html}, false);
  } else if (typeof options.uri === 'function') {
    options.uri(function(uri) {
      options.uri = uri;
      this._makeCrawlerRequest(options);
    }.bind(this));
  } else {
    this._makeCrawlerRequest(options);
  }
}

Crawler.prototype._pushToQueue = function _pushToQueue (options) {
  this.queueItemSize++;

  // you can use jquery or jQuery
  options = checkJQueryNaming(options);

  _.defaults(options, this.options);

  // Remove all the global options from our options
  // TODO we are doing this for every _pushToQueue, find a way to avoid this
  _.each(this.globalOnlyOptions, function(globalOnlyOption) {
    delete options[globalOnlyOption];
  });

  // If duplicate skipping is enabled, avoid queueing entirely for URLs we already crawled
  if (options.skipDuplicates && this.cache[options.uri]) {
    return this.emit('pool:release', options);
  }

  // acquire connection - callback function is called
  // once a resource becomes available
  this.pool.acquire(_handlePoolAcquire.bind(this, options), options.priority);
};

Crawler.prototype._makeCrawlerRequest = function _makeCrawlerRequest (options) {
  if (typeof options.rateLimits === 'number' && options.rateLimits !== 0) {
    setTimeout(this._executeCrawlerRequest.bind(this, options), options.rateLimits);
  } else {
    this._executeCrawlerRequest(options);
  }
};

Crawler.prototype._executeCrawlerRequest = function _executeCrawlerRequest (options) {
  var cacheData = this.cache[options.uri];

  //If a query has already been made to this URL, don't callback again
  if (useCache(options) && cacheData) {

    // Make sure we actually have cached data, and not just a note
    // that the page was already crawled
    if (_.isArray(cacheData)) {
      this._onContent(null, options, cacheData[0], true);
    } else {
      this.emit('pool:release', options);
    }

  } else {
    this._buildHttpRequest(options);
  }
};

Crawler.prototype._buildHttpRequest = function _buildHTTPRequest (options) {
  if (options.debug) {
    console.log(options.method+' '+options.uri+' ...');
  }

  // Cloning keeps the opts parameter clean:
  // - some versions of "request" apply the second parameter as a
  // property called "callback" to the first parameter
  // - keeps the query object fresh in case of a retry
  // Doing parse/stringify instead of _.clone will do a deep clone and remove functions

  var ropts = JSON.parse(JSON.stringify(options));

  if (!ropts.headers) { ropts.headers={}; }
  if (ropts.forceUTF8) {
    if (!ropts.headers['Accept-Charset'] && !ropts.headers['accept-charset']) {
      ropts.headers['Accept-Charset'] = 'utf-8;q=0.7,*;q=0.3';
    }
    if (!ropts.encoding) {
      ropts.encoding=null;
    }
  }
  if (typeof ropts.encoding === 'undefined') {
    ropts.headers['Accept-Encoding'] = 'gzip';
    ropts.encoding = null;
  }
  if (ropts.userAgent) {
    ropts.headers['User-Agent'] = ropts.userAgent;
  }
  if (ropts.referer) {
    ropts.headers.Referer = ropts.referer;
  }
  if (ropts.proxies && ropts.proxies.length) {
    ropts.proxy = ropts.proxies[0];
  }

  var requestArgs = ['uri','url','qs','method','headers','body','form','json','multipart','followRedirect',
                     'followAllRedirects', 'maxRedirects','encoding','pool','timeout','proxy','auth','oauth','strictSSL',
                     'jar','aws'];


  var req = request(_.pick.apply(this,[ropts].concat(requestArgs)), function(error,response) {
    if (error) {
      return this._onContent(error, options);
    }

    response.uri = response.request.href;

    // Won't be needed after https://github.com/mikeal/request/pull/303 is merged
    if (
      response.headers['content-encoding'] &&
        response.headers['content-encoding'].toLowerCase().indexOf('gzip') >= 0
    ) {
      zlib.gunzip(response.body, function (error, body) {
        if (error) {
          return this.onContent(error, options);
        }

        if (!options.forceUTF8) {
          response.body = body.toString(req.encoding);
        } else {
          response.body = body;
        }

        this._onContent(error,options,response,false);
      }.bind(this));
    } else {
      this._onContent(error,options,response,false);
    }
  }.bind(this));
  req.setMaxListeners(0);
};

function _handleInject(options, response, errors, $) {
  this._onInject(errors, options, response, $);
}

Crawler.prototype._onContent = function _onContent (error, options, response, fromCache) {
  if (error) {
    if (options.debug) {
      console.log('Error '+error+' when fetching '+
                  options.uri+(options.retries?' ('+options.retries+' retries left)':''));
    }
    if (options.retries) {
      this.plannedQueueCallsCount++;
      setTimeout(function() {
        options.retries--;
        this.plannedQueueCallsCount--;

        // If there is a "proxies" option, rotate it so that we don't keep hitting the same one
        if (options.proxies) {
          options.proxies.push(options.proxies.shift());
        }

        this.queue(options);
      }.bind(this), options.retryTimeout);

    } else if (options.callback) {
      options.callback(error);
    }

    return this.emit('pool:release', options);
  }

  if (!response.body) { response.body=''; }

  if (options.debug) {
    console.log('Got '+(options.uri||'html')+' ('+response.body.length+' bytes)...');
  }

  if (options.forceUTF8) {
    //TODO check http header or meta equiv?
    var iconvObj;

    if (!options.incomingEncoding) {
      var detected = jschardet.detect(response.body);

      if (detected && detected.encoding) {
        if (options.debug) {
          console.log(
            'Detected charset ' + detected.encoding +
              ' (' + Math.floor(detected.confidence * 100) + '% confidence)'
          );
        }
        if (detected.encoding !== 'utf-8' && detected.encoding !== 'ascii') {

          if (iconv) {
            iconvObj = new iconv(detected.encoding, 'UTF-8//TRANSLIT//IGNORE');
            response.body = iconvObj.convert(response.body).toString();

            // iconv-lite doesn't support Big5 (yet)
          } else if (detected.encoding !== 'Big5') {
            response.body = iconvLite.decode(response.body, detected.encoding);
          }

        } else if (typeof response.body !== 'string') {
          response.body = response.body.toString();
        }

      } else {
        response.body = response.body.toString('utf8'); //hope for the best
      }
    } else { // do not hope to best use custom encoding
      if (iconv) {
        iconvObj = new iconv(options.incomingEncoding, 'UTF-8//TRANSLIT//IGNORE');
        response.body = iconvObj.convert(response.body).toString();
        // iconv-lite doesn't support Big5 (yet)
      } else if (options.incomingEncoding !== 'Big5') {
        response.body = iconvLite.decode(response.body, options.incomingEncoding);
      }
    }

  } else {
    response.body = response.body.toString();
  }

  if (useCache(options) && !fromCache) {
    if (options.cache) {
      this.cache[options.uri] = [response];

      //If we don't cache but still want to skip duplicates we have to maintain a list of fetched URLs.
    } else if (options.skipDuplicates) {
      this.cache[options.uri] = true;
    }
  }

  if (!options.callback) {
    return this.emit('pool:release', options);
  }

  response.options = options;

  // This could definitely be improved by *also* matching content-type headers
  var isHTML = response.body.match(/^\s*</);

  if (isHTML && options.jQuery && options.method !== 'HEAD') {
    this._inject(response, options, _handleInject.bind(this, options, response));
  } else {
    options.callback(null,response);
    this.emit('pool:release', options);
  }
};

Crawler.prototype._onInject = function _onInject (errors, options, response, $) {
  options.callback(errors, response, $);
  this.emit('pool:release', options);
};

module.exports = Crawler;
module.exports.VERSION = '0.3.1';
