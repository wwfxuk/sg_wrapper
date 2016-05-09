Changelog Api
=============

Releases
--------

Version 1.3.2
````````````````
- sg_wrapper_util.get_calling_script: Ignore ipython from the stacktrace

Version rc-1.3.1
````````````````
- sg_wrapper.Shotgun.update: fixed typo


Version rc-1.3
``````````````
- New sgw.update behaviour: if a dict is provided instead of a list, update the entity fields according to the dict values
- sg_wrapper.Shotgun automatically retrieve the api key if a script name is provided
- sg_wrapper.Shotgun modifies the shotgun_api3 instance if it was provided:
  - it replaces session_uuid with the current username as an uuid
  - if no script name was provided, it guesses it from the stack trace and update the script name and the api key to match it
- sg_wrapper_util.get_user_from_event(eventId) allows one to retrieve the username from an event
- userFromEvent script allows to retrieve the user infos from a shotgun event


Version 1.2
```````````
- New sgw.update behaviour: if a dict is provided instead of a list, update the entity fields according to the dict values
- Add optional args (displayName, tagList) to Entity.upload function to fill with Shotgun api
- Entity.upload docstring
