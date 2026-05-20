from neomodel import (
    AsyncStructuredNode,
    BooleanProperty,
    DateTimeProperty,
    EmailProperty,
    StringProperty,
)


class EmailContact(AsyncStructuredNode):
    email = EmailProperty(required=True, unique_index=True, max_length=255)
    confirmed = BooleanProperty(default=False)
    email_confirmed_at = DateTimeProperty()


class PhoneContact(AsyncStructuredNode):
    phone_number = StringProperty(required=True, unique_index=True, max_length=40)


class SocialMediaContact(AsyncStructuredNode):
    twitter_url = StringProperty(max_length=255)
    linkedin_url = StringProperty(max_length=255)
    facebook_url = StringProperty(max_length=255)
    instagram_url = StringProperty(max_length=255)
    youtube_url = StringProperty(max_length=255)
    tiktok_url = StringProperty(max_length=255)
