from flask import Flask, render_template
app = Flask(__name__)

@app.route('/main')
def mainpage():
    return app.send_static_file("main.html")

if __name__ == "__main__":
    app.run()