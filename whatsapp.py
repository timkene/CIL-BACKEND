import requests

WHATSAPP_TOKEN = "EAALZB3ZAwCztoBQ5xq2Fw6WxeOWj5dDEYddFXWlqsbV0bvtMceMANoOdZBZCJt1lZBNsnS0l2UrX90ZBXxXvt4MMEYLaZBFXn9RZAFeNXMaXPZBZChb5Oa3lyqkV3ZCZCgpXhBk0SFpxVf3jPtvrC59vpvuVPVJzzAFaDH5k4s9XK6txwmI3rYQOHbmJXbntQjHZAP7HZClQ7VQjYSwBVplKeiahhwtlDlz5ZBKTzF12Cn0s2TT2WAOOQqIChYb78Yh5mUCXHm5pl86be2zN6X2lhtwowu39oVTtSQcPEIuGZCcTPAZDZD"
PHONE_NUMBER_ID = "1042221315634908"
YOUR_NUMBER = "2348148599362"  # your number with country code, no +

def send_whatsapp_alert(message: str):
    """Send a free-form text message (requires 24h customer service window or production mode)."""
    url = f"https://graph.facebook.com/v22.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": YOUR_NUMBER,
        "type": "text",
        "text": {"body": message}
    }
    response = requests.post(url, json=payload, headers=headers)
    return response.json()

def send_whatsapp_template(template_name: str = "hello_world", language: str = "en_US"):
    """Send a pre-approved template message. Works in dev mode with any number."""
    url = f"https://graph.facebook.com/v22.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": YOUR_NUMBER,
        "type": "template",
        "template": {"name": template_name, "language": {"code": language}}
    }
    response = requests.post(url, json=payload, headers=headers)
    return response.json()

# Test it
if __name__ == "__main__":
    # Use template for dev mode testing (works without verified test contact)
    result = send_whatsapp_template("hello_world")
    print(result)