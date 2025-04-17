Activate environment: 
`source viirs_to_awips/bin/activate`
* install packages using pip

Update requirements file: 
`pip freeze > env_viirs_to_awips.txt`

Recreate environment from requirements file: 
`python -m venv viirs_to_awips`
`source viirs_to_awips/bin/activate`
`pip install -r env_viirs_to_awips.txt`