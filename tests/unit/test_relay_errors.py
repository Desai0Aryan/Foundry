import aio_pika

from foundry.relay import UnroutableError, is_infrastructure_error


def test_broker_outages_do_not_count_toward_the_poison_cap():
    assert is_infrastructure_error(ConnectionResetError())
    assert is_infrastructure_error(TimeoutError())
    assert is_infrastructure_error(aio_pika.exceptions.AMQPConnectionError("down"))
    assert is_infrastructure_error(aio_pika.exceptions.ChannelInvalidStateError("closed"))


def test_message_specific_failures_still_count():
    assert not is_infrastructure_error(UnroutableError("no queue"))
    assert not is_infrastructure_error(ValueError("bad payload"))
