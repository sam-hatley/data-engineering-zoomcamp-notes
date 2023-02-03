from prefect.blocks.notifications import SlackWebhook

slack_webhook_block = SlackWebhook.load("slack-block")
slack_webhook_block.notify("Hello!")