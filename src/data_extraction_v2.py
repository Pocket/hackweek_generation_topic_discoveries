import mastodon
from mastodon import Mastodon
from dotenv import dotenv_values


def create_app():
    '''Create a Mastodon App'''
    Mastodon.create_app(
    'hack-posttoots-social',
    api_base_url = 'https://mastodon.social',
    # api_base_url = 'https://mozilla.social',
    scopes=['read', 'write'],
    to_file = '.secrets')
    mastodon = Mastodon (client_id = '.secrets')
    return mastodon

def get_access_token(mastodon):
    '''Get access token for the Mostodon App'''
    config = dotenv_values(".env")
    access_token = mastodon.log_in(
        username = config['USERNAME'],
        password = config['PASSWORD'],
        scopes = ['read', 'write']
    )
    
    return access_token



if __name__ == '__main__':
    mastodon = create_app()
    mastodon.access_token = get_access_token(mastodon)
    # mastodon.status_post('Tooting from the command line!')
