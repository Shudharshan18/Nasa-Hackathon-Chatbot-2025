from flask import Flask,render_template,request,jsonify,session,redirect,url_for
from mysql_connect import getdata
import retreval
import uuid

app = Flask(__name__)
app.secret_key = str(uuid.uuid4())  # Generate a random secret key for session management

conversations = {}

@app.before_request
def ensure_session():
    if "session_id" not in session:
        session["session_id"] = str(uuid.uuid4())
        conversations[session["session_id"]] = []



@app.route('/ask',methods=['POST'])
def ask():
    json = request.get_json()
    message = json.get('message')
    sid = session["session_id"]
    history = conversations[sid]
    context = "\n".join(history[-3:])

    # print(message)
    response = retreval.retrieve(message)

    history.append(f"User: {message}")
    history.append(f"Patuta: {response}")
    # print(response)
    return jsonify({"reply":response})

    
@app.route('/data',methods=['GET'])
def data():
    query = "SELECT year,emissions FROM fossil_fuel"
    data = getdata(query)
    return jsonify({"reply":data})
@app.route('/')
def home():
    return render_template('home.html')

@app.route('/emission')
def emission():
    return render_template('emission.html')

@app.route('/impacts')
def impacts():
    return render_template('impacts.html')

@app.route('/analysis')
def analysis():
    return render_template('analysis.html')

@app.route('/chatbot')
def chatbot():
    return render_template('index.html')
if __name__ == '__main__':
    app.run(debug=True)