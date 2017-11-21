import settings

from sqlalchemy import create_engine

engine = create_engine(settings.MYSQL_DSN_CELERY)

