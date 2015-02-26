from _subprocess import CREATE_NEW_PROCESS_GROUP
import subprocess
from pyLibrary.jsons import Log


def get_git_revision():
    """
    GET THE CURRENT GIT REVISION
    """
    proc = subprocess.Popen(
        ["git", "status"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=-1,
        creationflags=CREATE_NEW_PROCESS_GROUP
    )

    while True:
        line = proc.stdout.readline()
        if not line:
            continue
        if line.find(" * Running on") >= 0:
            server_is_ready.go()
        Log.note("SERVER: {{line}}", {"line": line.strip()})

