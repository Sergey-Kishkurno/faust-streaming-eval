import random
import faust

app = faust.App("consumer-demo")
greetings_topic = app.topic(
    "greetings",
    value_type=str,
    value_serializer="json"
)


@app.timer(interval=5)
async def generate_greeting():
    prefix = random.choice(["Hi, ", "Hello, ", "Howdy, "])
    name = random.choice(["Joe!",  "Rebecca!", "Sam!", "Bob!"])
    await greetings_topic.send(value=f"{prefix} {name}")


@app.agent(greetings_topic)
async def process_greetings(stream):
    async for greeting in stream:
        print(f"Greeting: {greeting}")
