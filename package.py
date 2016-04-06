name = 'sg_wrapper'
version = '0.0.0.mikros.2.0'

requires = ['shotgunPythonApi']

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

    env.PYTHONPATH.append('{root}')
