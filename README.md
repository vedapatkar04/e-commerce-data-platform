Always check for port conflicts before debugging credentials.
If a service is already running on the same port, Docker's 
container will never receive the connection — even if everything 
else is perfectly configured.

Rule: One service per port on your machine.