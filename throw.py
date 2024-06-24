def test():
    try:
        a = 1 / 0
    except:
        raise Exception
    finally:
        return


if __name__ == '__main__':
    try:
        test()
    except:
        print('예외 잘 잡니?')
