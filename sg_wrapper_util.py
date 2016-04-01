import os
import warnings

def get_calling_script():
    ''' Retrieve the calling script name by exploring the stack.
        The following frames in the stack are ignored:
            * the parents of the frame called by __load_apps
            * the frame initialized by <stdin> (ie python commandline)
            * the frame initialized by */IPython/* (ie ipython commandline)
        
        :return: The last non-ignored frame (ie the oldest) or None if none was found
        :rtype: str

        .. note:: The name of a frame is either, by priority:
            * The name of its file, if the commandline first argument (ie the script name) is equal to its file. This allows to get the script name instead of the package name if it was called from a script.
            * The name of the package containing its file, if any was found
            * The name of its file

        >>> get_calling_script()
        'pythonStandalone'
    '''

    from inspect import stack
    import sys

    _stack = stack()

    # find the last frame from a proper package

    convertedStack = map(lambda frame: get_script_name_from_frame(frame), _stack)

    # cut everything after the first 'recurs_ignore' encountered - cf get_script_name_from_frame
    cut = len(convertedStack)
    for i in range(len(convertedStack)):
        if convertedStack[i] == (None, 'recurs_ignore'):
            cut = i
            break
    convertedStack = convertedStack[:cut]

    for frame in convertedStack[::-1]:
        filename, scriptName = frame

        # if the commandline argument is equal to the first proper frame, we assume it's a
        #   direct script call from the commandline, so we log the script name instead of the package
        if filename:
            try:
                if len(sys.argv) > 0:
                    cmdFilePath = sys.argv[0]
                    cmdFileName = os.path.basename(cmdFilePath)
                    cmdFileNameParsed = os.path.splitext(cmdFileName)
                    if len(cmdFileNameParsed) > 0:
                        cmdFileNameWithoutExtension = cmdFileNameParsed[0]
                        if filename == cmdFileNameWithoutExtension:
                            return filename

            except AttributeError:  # handle dirty stack
                pass

        if scriptName:
            return scriptName

        if filename:
            return filename

    # TODO throw exception ? or default script name
    return None

def get_script_name_from_frame(frame):
    ''' Return the filename and the package name of a frame, or (None,None) if it should be ignored.
        The following frames in the stack are ignored:
            * the parents of the frame called by __load_apps
            * the frame initialized by <stdin> (ie python commandline)
            * the frame initialized by */IPython/* (ie ipython commandline)

        :param frame: frame to retrieve the info from
        :type frame: frame
        :return: (filename, packageName), (None,None) if the frame is ignored and (None,'recurs_ignore') if its parents should be ignored too
        :rtype: (str,str)
    '''

    # return a tuple (filename, packageName)
    #   or (None, None) if it should be ignored
    #   or (None, 'recurs_ignore') if it and all it's parents should be ignored

    if len(frame) < 4:
        return (None, None)

    # some (every?) commandline utilies call script from __load_apps - we ignore every parent's frame if a frame comes it
    if frame[3] == '__load_apps':
        return (None, 'recurs_ignore')

    path = frame[1]

    # ignore ipython & python commandline frame
    if(path.startswith('python')
            or '/ipython/' in path
            or '<stdin>' == path):
        return (None, None)

    filename = os.path.abspath(path)

    name = os.path.basename(filename)
    nameWithoutExtension = os.path.splitext(name)[0]

    # search for package.py in parent folders
    lastFolder = filename
    folder = os.path.dirname(filename)
    while folder != lastFolder:
        if os.path.exists(os.path.join(folder, 'package.py')):
            # folder contains package.py: it's the version folder
            return (nameWithoutExtension, os.path.basename(os.path.dirname(folder)))
        lastFolder = folder
        folder = os.path.dirname(folder)

    return (nameWithoutExtension, None)

def get_user_from_event(eventId, sgw=None):
    ''' Get the user that called the script causing an event

    :param eventId: The id of the event the user must be retrieve from
    :type eventId: int
    :param sgw:
        sg_wrapper handle.
        If none is provided, the scripts tries to instantiate one.
    :type sgw: sg_wrapper.Shotgun

    :return:
        The username of the user who called the script causing this event
        or None if it could not be retrieved
    :rtype: str

    :raise:
        ValueError: if the event doesn't exist
        RuntimeError: if a sg_wrapper handle is not provided and its instantiation failed
    '''

    if not sgw:
        import re
        import sg_wrapper
        import tank

        if 'PROD' not in os.environ:
            raise RuntimeError(("Impossible to initialize an sg_wrapper's shotgun handle"
                                "without the PROD environment variable set"))

        project = re.sub('\s+', '', os.environ['PROD']).upper()
        projectEnv = 'PC_%s' % project

        if projectEnv not in os.environ:
            raise RuntimeError(("Impossible to initialize an sg_wrapper's shotgun handle"
                                "without the %s environment variable set") % projectEnv)

        try:
            tk = tank.tank_from_path(os.environ[projectEnv])
        except tank.TankError as e:
            raise RuntimeError(("Impossible to initialize an sg_wrapper's shotgun handle"
                                "as the tank handle could not be initiliazed: %s") % e.strerro)

        sgw = sg_wrapper.Shotgun(sg=tk.shotgun)


    ev = sgw.sg_find_one('EventLogEntry', [['id', 'is', eventId]], ['session_uuid'])

    if ev is None:
        raise ValueError('Could not find EventLogEntry %s in Shotgun' % eventId)

    _uuid = ev['session_uuid']

    if _uuid is None:
        warnings.warn('Unable to retrieve the user from the EventLogEntry %s' % eventId)
        return None

    try:
        username = uuid_to_string(_uuid)
    except ValueError:
        warnings.warn('Unable to retrieve the user from the EventLogEntry %s' % eventId)
        return None

    return username

def string_to_uuid(_string):
    ''' Return an UUID string based on the input. Opposite of uuid_to_string.

    :param _string: string to encoded
    :type _string: str

    :return: _string encoded as an UUID string
    :rtype: str

    >>> string_to_uuid('doctest')
    '646f6374-6573-4740-a000-000000000000'
    >>> warnings.simplefilter('ignore')  # disable warnings, to not display the truncated-string message
    >>> string_to_uuid('doctestwayyyyytolong')
    '646f6374-6573-4747-b761-797979797974'
    '''

    # uuid version 4 :
    #     xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
    #     where x in an hexadecimal digit and y is either 8, 9, a or b
    # we use the remaining 30 bytes (x) to store the string
    # converted as hexadecimal, with trailing zeros
    # if the string is too long, we truncate it and set y='b', otherwise y='a'

    _hex = _string.encode('hex')

    isTruncated = len(_hex) > 30
    if isTruncated:
        warnings.warn('Warning: only the 15 first character of %s (%s) will be encoded as an UUID'
              % (_string, _string[:15]))

    return '{:0<8}-{:0<4}-4{:0<3}-{y}{:0<3}-{:0<12}'.format(
                                                          _hex[:8],
                                                          _hex[8:12],
                                                          _hex[12:15],
                                                          _hex[15:18],
                                                          _hex[18:30],
                                                          y='b' if isTruncated else 'a')

def uuid_to_string(_uuid):
    ''' Extract a string from an UUID. Opposite of string_to_uuid.

    :param _uuid: UUID to extract the string from
    :type _uuid: str

    :return: extracted string from _uuid
    :rtype: str

    :raise:
        ValueError if the UUID not valid

    >>> uuid_to_string('646f6374-6573-4740-a000-000000000000')
    'doctest'
    >>> warnings.simplefilter('ignore')  # disable warnings, to not display the truncated-string message
    >>> uuid_to_string('646f6374-6573-4747-b761-797979797974')
    'doctestwayyyyyt'
    '''

    # uuid version 4 :
    #     xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
    #     where x in an hexadecimal digit and y is either 8, 9, a or b
    # we use the remaining 30 bytes (x) to store the string
    # converted as hexadecimal, with trailing zeros
    # if the string is too long, we truncate it and set y='b', otherwise y='a'

    from string import hexdigits

    if (len(_uuid) != 36
            or any(_uuid[i] != '-' for i in [8, 13, 18, 23])
            or _uuid[14] != '4'
            or (_uuid[19] != 'a' and _uuid[19] != 'b')):
        raise ValueError('Could not extract a string from the non valid UUID %s' % _uuid)

    _hex = _uuid[:8] + _uuid[9:13] + _uuid[15:18] + _uuid[20:23] + _uuid[24:]

    if any(hexchar not in hexdigits for hexchar in _hex):
        raise ValueError('Could not extract a string from the non valid UUID %s' % _uuid)

    if _uuid[19] == 'b':
        warnings.warn('Warning: %s only encoded part of a string' % _uuid)

    return _hex.decode('hex').rstrip('\x00')
