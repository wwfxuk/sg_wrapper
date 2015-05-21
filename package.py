name = 'sg_wrapper'
version = '0.0.0.mikros.1.1'

requires = ['shotgunPythonApi']

def commands():

    env.PYTHONPATH.append('{root}')
