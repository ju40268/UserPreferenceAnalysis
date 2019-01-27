import smtplib
def report(error_msg):
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.starttls()
	server.login("ju40268@gmail.com", "#")
	msg = error_msg
	server.sendmail("ju40268@gmail.com", "ju40268@gmail.com", msg)
	server.quit()