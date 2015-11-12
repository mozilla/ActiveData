

import os
from pyLibrary.env.files import File

for subdir, dirs, files in os.walk("c:/users/kyle/code"):
    for file in files:
        #print os.path.join(subdir, file)
        filepath = subdir + os.sep + file

        if ".idea" in filepath or filepath.endswith(".iml"):
            file = File(filepath)
            newfile = File(file.abspath.replace("c:/users/kyle/", "e:/"))
            newfile.write_bytes(file.read_bytes())
