class Key(str):
    def __getitem__(self, key):
        return Key("%s:%s" % (self, key,))

if __name__ == '__main__':
    key = Key(object='Model')
    print key['all']['b']
    pass
