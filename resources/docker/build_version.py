import os

from pyLibrary.env.git import get_git_revision

from mo_dots import coalesce

from mo_files import File
from mo_json import value2json

File("version.json").write(value2json(
    {
        "source": os.environ.get('REPO'),
        "version": coalesce(os.environ.get('TAG'), os.environ('BRANCH')),
        "commit": get_git_revision(),
        "build": os.environ.get('CIRCLE_BUILD_URL')
    }
))
