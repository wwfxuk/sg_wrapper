name = 'sg_wrapper'
version = 'internal.0.0.0.mikros.2.1.0'

requires = [
    'shotgunPythonApi',
    'carbine',
    'inflection',
]

custom = {
        'description': 'Wrapper around shotgun',
        # 'doc': '',
        # 'wiki': 'http://wiki.mikros.int/doku.php?id=anim:dev:sg_wrapper',
        'authors': ['syd', 'jbi'],
        'maintainers': ['syd', 'jbi'],
        }

def commands():

    import os

    env.PYTHONPATH.append('{root}')

    alias('userFromEvent', os.path.join(this.root, 'scripts', 'userFromEvent.py'))
    alias('evusr', 'userFromEvent')
