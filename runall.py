import json, subprocess, os, signal

if __name__ == '__main__':
    with open('config.json') as f:
        J = json.loads(f.read())
    nServer = int(J['nservers'])
    servers = []
    try:
        for i in range(1, nServer + 1):
            servers.append(subprocess.Popen(['python', 'main.py', '--id={}'.format(i)]))
        for server in servers:
            server.wait()
    except (KeyboardInterrupt, SystemExit):
        for server in servers:
            os.kill(server.pid, signal.SIGKILL)