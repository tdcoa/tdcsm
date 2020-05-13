from flask import Flask

app = Flask(__name__, static_folder='static', template_folder='templates')
app.config.from_pyfile('config.py', silent=True)

from tdcsm.flask_server import routes

app.run(debug=True, port=8888)
