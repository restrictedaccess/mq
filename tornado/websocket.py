import tornado.web
import tornado.websocket
import tornado.httpserver
import tornado.ioloop
import tornado.options
from uuid import uuid4
import json
from tornado.options import define, options
define("port", default=8000, help="run on the given port", type=int)

class NotificationCenter(object):

	callbacks = []
	admin_id = 0

	def register(self, callback):
		self.callbacks.append(callback)
		
	def unregister(self, callback):
		self.callbacks.remove(callback)

	def notifyCallbacks(self, admin_id):
		for callback in self.callbacks:
			callback(admin_id)
			
	def ping(self, admin_id):
		self.notifyCallbacks(admin_id)

class PingHandler(tornado.web.RequestHandler):
	def get(self):
		admin_id = self.get_argument('admin_id')
		self.application.notificationCenter.ping(admin_id)
	
class NotificationHandler(tornado.websocket.WebSocketHandler):
	
	def open(self):
		self.application.notificationCenter.register(self.callback)
		
	def on_close(self):
		self.application.notificationCenter.unregister(self.callback)
		
	def on_message(self, message):
		pass
		
	def callback(self, admin_id):
		self.write_message('{"admin_id":"%s"}' % admin_id)
	

class Application(tornado.web.Application):
	def __init__(self):
		self.notificationCenter = NotificationCenter()
		
		handlers = [
		(r'/sms/notif/', NotificationHandler),
		(r'/sms/ping/', PingHandler),

		]
		settings = {
		'template_path': 'templates',
		'static_path': 'static'
		}
		tornado.web.Application.__init__(self, handlers, **settings)
		
if __name__ == '__main__':
	tornado.options.parse_command_line()
	app = Application()
	server = tornado.httpserver.HTTPServer(app)
	server.listen(8000)
	tornado.ioloop.IOLoop.instance().start()

