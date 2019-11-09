from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers

from mo_dots import Data
from mo_json import value2json, json2value
from mo_math import bytes2base64, base642bytes, int2base64, base642int


def generate_key(bits=512):
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=bits,
        backend=default_backend()
    )
    nums = private_key.public_key().public_numbers()
    public_key = Data(e=nums.e, n=int2base64(nums.n))
    return public_key, private_key


def sign(message, private_key):
    data = value2json(message).encode('utf8')

    # SIGN DATA/STRING
    signature = private_key.sign(
        data=data,
        padding=padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        algorithm=hashes.SHA256()

    )

    return Data(
        data=bytes2base64(data),
        signature=bytes2base64(signature)
    )


def verify(signed, public_key):
    data = base642bytes(signed.data)
    signature = base642bytes(signed.signature)

    key = RSAPublicNumbers(public_key.e, base642int(public_key.n)).public_key(default_backend())
    key.verify(
        signature=signature,
        data=data,
        padding=padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        algorithm=hashes.SHA256()
    )

    return json2value(data.decode('utf8'))
