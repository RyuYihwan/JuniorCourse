import threading

# 공유 변수
counter = 0


def increase_counter():
    global counter
    for _ in range(1000000):
        counter += 1


# 두 개의 스레드 생성
thread1 = threading.Thread(target=increase_counter)
thread2 = threading.Thread(target=increase_counter)

# 스레드 시작
thread1.start()
thread2.start()

# 두 스레드가 종료되기를 기다림
thread1.join()
thread2.join()

print(f"예상되는 값: {2 * 1000000}, 실제 counter 값: {counter}")
