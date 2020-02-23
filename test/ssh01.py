import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(hostname='somehost', username='somename', password='passwd')

stdin, stdout, stderr = client.exec_command('ls')

print(stdout.readlines())
client.close()
