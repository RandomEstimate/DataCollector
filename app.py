from flask import Flask,request
import json
import os
app = Flask(__name__)


@app.route('/', methods=['GET','POST'])
def git_update():
    if json.loads(request.form.to_dict()['payload'])['commits'][0]['message'] == 'version_update':
        os.system('git pull')
        if os.path.exists('pidfile.txt'):
            os.system("kill - 9 `cat pidfile.txt`")
        os.system('nohup python acquisition.py > logfile.txt & echo $! > pidfile.txt')
        os.system('git add .')
        os.system('git commit -m "daily update"')
        os.system('git push')
    return 'git_update'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5555)
