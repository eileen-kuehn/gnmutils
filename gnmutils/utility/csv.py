import os


def dumpToFileName(folder, filename, data, header=None):
    if not os.path.exists(folder):
        os.makedirs(folder)
    with open(os.path.join(folder, filename), 'a') as file:
        if file.tell() == 0:
            # beginning of file so insert header
            file.write(header+"\n")
        file.write(data+"\n")


def dumpToFile(file=None, data=None, header=None):
    if file.tell() == 0:
        file.write(header+"\n")
    file.write(data+"\n")
