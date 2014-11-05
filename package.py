name = 'sg_wrapper'
version = '1.0'

requires = ['shotgunPythonApi']

def commands():

    env.PYTHONPATH.append('{root}')
