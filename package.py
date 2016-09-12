name = 'sg_wrapper'
version = 'rc.0.0.0.mikros.2.0.2'

requires = [
    'shotgunPythonApi',
    'carbine',
]

custom = {
        'description': 'Wrapper around shotgun',
        'doc': '',
        'wiki': 'http://wiki.mikros.int/doku.php?id=anim:dev:sg_wrapper',
        'wikiOthers': [],
        'authors': ['syd'],
        'authorEmails': ['syd@mikrosimage.eu'],
        'maintainers': ['syd'],
        'maintainerEmails': ['syd@mikrosimage.eu'],
        'sourcePackage': '',
        'deployStrategy': 'git',
        'synchroStrategy': 'git',
        }

def commands():

    import os

    env.PYTHONPATH.append('{root}')

    alias('userFromEvent', os.path.join(this.root, 'scripts', 'userFromEvent.py'))
    alias('evusr', 'userFromEvent')
