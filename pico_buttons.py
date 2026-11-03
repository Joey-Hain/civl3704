"""MicroPython firmware for USB buttons that control the delay dashboard.

Copy this file to a Raspberry Pi Pico as ``main.py``. Wire momentary buttons
between GPIO 15, 14, and 13 respectively and GND; the internal pull-ups keep
each input high while idle. When the dashboard computer has the USB keyboard
interface open, a press sends the matching A, B, or C key:

* A cycles the heatmap layer.
* B cycles the heatmap metric.
* C toggles bus markers.

This is a continuously running device script, not a host-importable Python
module: it depends on MicroPython's ``machine`` and ``usb`` modules.
"""

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
    """Send one USB key press followed by a release.

    The short pause gives the host time to register the press while the
    explicit release prevents the keyboard from repeating the key.

    Args:
        key: A ``KeyCode`` value accepted by ``KeyboardInterface.send_keys``.
    """
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