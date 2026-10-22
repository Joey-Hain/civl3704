"""MicroPython firmware for three debounced USB-serial dashboard buttons.

Connect each button between its GPIO pin and GND. Copy this file to the
Pico as main.py so it starts automatically when the board is powered.
"""

from machine import Pin
from time import sleep_ms, ticks_diff, ticks_ms

BUTTON_PINS = (14, 15, 16)
DEBOUNCE_MS = 50
POLL_MS = 10

buttons = [Pin(pin, Pin.IN, Pin.PULL_UP) for pin in BUTTON_PINS]
last_readings = [button.value() for button in buttons]
stable_readings = last_readings[:]
changed_at = [ticks_ms() for _ in buttons]

while True:
    now = ticks_ms()
    for index, button in enumerate(buttons):
        reading = button.value()
        if reading != last_readings[index]:
            last_readings[index] = reading
            changed_at[index] = now
        elif (
            reading != stable_readings[index]
            and ticks_diff(now, changed_at[index]) >= DEBOUNCE_MS
        ):
            stable_readings[index] = reading
            if reading == 0:
                print("B{}".format(index + 1))
    sleep_ms(POLL_MS)
