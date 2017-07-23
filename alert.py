import smtplib
def report(error_msg):
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login("ju40268@gmail.com", "may811204")
    msg = error_msg
    server.sendmail("cchen19@logitech.com", "ju40268@gmail.com", msg)
    server.quit()
#import yagmail
#yag = yagmail.SMTP('ju40268@gmail.com', 'may811204')
#yag.send(contents = 'Hi')

#import yagmail
#yagmail.register('ju40268', 'may811204')
#yag = yagmail.SMTP('ju40268')
# yag = yagmail.SMTP()
#contents = ['This is the body, and here is just text http://somedomain/image.png',
#            'You can find an audio file attached.', '/local/path/song.mp3']
#yag.send('ju40268@gmail.com', 'subject', contents)

# report('Hi')




