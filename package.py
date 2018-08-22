name = 'sg_wrapper'
version = '0.0.0.wwfx.1.0.0'

requires = ['shotgun_api']

authors = [
    'Sylvain Delhomme',
    'Joran Bigalet',
    'Joseph Yu',
]

description = 'Wrapper around shotgun'

def commands():
    import os
    env.PYTHONPATH.append('{root}')
    alias('userFromEvent', os.path.join(this.root, 'scripts', 'userFromEvent.py'))
    alias('evusr', 'userFromEvent')

build_command = r"""

cp -r {root}/sg_wrapper* {root}/scripts $REZ_BUILD_PATH/

# If install as rez package, copy shotgun_api3 to package directory
if [[ "{install}" ]]
then
    cp -r $REZ_BUILD_PATH/* $REZ_BUILD_INSTALL_PATH/
fi

"""