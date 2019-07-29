import sys
import socket
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


class TweetsListener(StreamListener):

    def __init__(self, socket):

        print("Tweets listener initialized")
        self.client_socket = socket

    def on_data(self, data):

        try:
            jsonMessage = json.loads(data)
            message = jsonMessage("text").encode("utf-8")
            print(message)
            self.client_socket.send(message)

        except BaseException as e:
            print("Error on_data: %s" % str(e))

        return True

    def on_error(self, status):

        print(status)
        return True


def connect_to_twitter(connection, tracks):

    api_key = " "  # the api_key from the twitter development account needs to be provided here
    api_secret = " "  # the api_secret from the twitter development account needs to be provided here

    access_token = " "  # the access_token from the twitter development account needs to be provided here
    access_token_secret = " "  # the access_token_secret from the twitter development account needs to be provided here

    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = Stream(auth, TweetsListener(connection))
    twitter_stream.filter(track=tracks, languages=["en"])


if __name__ == '__main__':

    if len(sys.argv) < 4:
        print("Usage: python twitterStreaming.py <hostname> <port> <tracks>",
              file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    tracks = sys.argv[3:]

    s = socket.socket()
    s.bind((host, port))

    print("Listenting on port: %s" % str(port))

    s.listen(5)

    connection, client_address = s.accept()

    print("Received request from: " + str(client_address))
    print("Initializing listener for these tracks: ", tracks)

    connect_to_twitter(connection, tracks)











