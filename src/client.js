class Client {
    constructor(clientId, clientType, config, topicConfig, emitter) {
        this.clientId = clientId;
        this.clientType = clientType;
        
        // common config defaults should go here.
        this.config = Object.assign({
            'metadata.broker.list': 'localhost:9092',
            'socket.keepalive.enable': true,
          }, 
          config,
          { 'client.id': clientId }
        );
        // commong topic configs defaults should go here. 
        this.topicConfig = topicConfig;
        this.emitter = emitter;
    }

    _logMessage(msgType, message, data) {
        if (this.emitter != null) {
            this.emitter.emit(msgType, {
                clientId: this.clientId,
                clientType: this.clientType,
                message,
                data,
            });
        } else if (msgType === 'error') {
            console.error(this.clientId, this.clientType, message, typeof data !== 'undefined' ? data : '');
        } else {
            console.log(this.clientId, this.clientType, message, typeof data !== 'undefined' ? data : '');
        }
    }

    log(message, data) {
        this._logMessage('log', message, data);
    }

    success(message, data) {
        this._logMessage('success', message, data);
    }

    error(err, data) {
        this._logMessage('error', err, data);
    }
}

module.exports = Client;