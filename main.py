import time
from machine import Pin
import usb.device
from usb.device.keyboard import KeyboardInterface, KeyCode

# GPIO pin -> key to send. Wire each button between the GPIO pin and GND.
BUTTONS = [
    (15, KeyCode.A),
    (14, KeyCode.B),
    (13, KeyCode.C),
]

# Internal pull-ups: pin reads 1 (HIGH) when idle, 0 (LOW) when pressed
buttons = [(gp, Pin(gp, Pin.IN, Pin.PULL_UP), key) for gp, key in BUTTONS]

kb = KeyboardInterface()
usb.device.get().init(kb, builtin_driver=True)  # keeps the USB serial REPL available


def tap(key):
    """Send one press and release, so the host never auto-repeats the key."""
    kb.send_keys([key])
    time.sleep_ms(20)
    kb.send_keys([])


last = [1] * len(buttons)

while True:
    readings = [pin.value() for gp, pin, key in buttons]

    if readings != last:
        print(" ".join("GP{}={}".format(gp, "HIGH" if v else "LOW")
                       for (gp, pin, key), v in zip(buttons, readings)))

        if kb.is_open():
            for (gp, pin, key), before, now in zip(buttons, last, readings):
                if before == 1 and now == 0:  # button just pressed
                    tap(key)

        last = readings
        time.sleep_ms(30)  # debounce

    time.sleep_ms(10)