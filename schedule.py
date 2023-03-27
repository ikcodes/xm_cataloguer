#===================================================================
# This script is the flashpoint for the server's CRON. That's it
#===================================================================

logOutput = True

from fido import runFido

spinsIngested = runFido(logOutput)

if(logOutput):
	print('INGESTED ' + str(spinsIngested) + ' NEW SPINS!')