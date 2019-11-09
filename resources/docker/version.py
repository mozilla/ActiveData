import os

from mo_files import File
from mo_future import text
from mo_json import value2json
from pyLibrary.env import git

print("Uses *.dockerfile environment variables to write a verion.json file")

File("version.json").write(value2json(
    {
        "source": os.environ.get('REPO_URL'),
        "version": os.environ.get('REPO_CHECKOUT').replace("tags/", '"'),
        "commit": git.get_revision(),
        "build": os.environ.get('BUILD_URL')
    },
    pretty=True
) + text("\n"))
