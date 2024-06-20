import threading
import time

"""
스레드는 threading.Thread 메서드를 사용해서 만들면 됨.
target에는 스레드로 띄울 함수명을 넣어주면 된다.
name으로 스레드의 이름을 지정 가능
args로 함수의 인자를 넘겨줄 수 있음.
"""


def worker(msg):
    """
    start 때 전달 받은 메세지 출력 10초 후에 종료
    """
    print('{} is started, msg: {}'.format(threading.current_thread().name, msg))
    time.sleep(10)
    print('{} is finished, msg: {}'.format(threading.current_thread().name, msg))


if __name__ == '__main__':
    for i in range(1, 11):
        msg = "{}번 스레드".format(i)
        # 스레드 생성
        t = threading.Thread(target=worker, name="t-{}".format(i), args=(msg,))
        # 스레드 시작
        t.start()

# 예제 1
# def sum(low, high):
#     total = 0
#     for i in range(low, high):
#         total += i
#     print('sub_thread', total)
#
#
# t1 = threading.Thread(target=sum, args=(1, 10))
# t2 = threading.Thread(target=sum, args=(1, 11))
# t3 = threading.Thread(target=sum, args=(1, 12))
#
# t1.start()
# t2.start()
# t3.start()
#
#
# print('main_thread')
