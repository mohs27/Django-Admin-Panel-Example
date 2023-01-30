
import uuid

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(_('created'), auto_now_add=True)
    modified = models.DateTimeField(_('modified'), auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):

    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True, null=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')

    def __str__(self):
        return self.name


class Filmwork(UUIDMixin, TimeStampedMixin):

    class FilmWorkType(models.TextChoices):
        MOVIE = 'movie', _('movie')
        TV_SHOW = 'tv_show', _('tv_show')

    title = models.TextField(_('title'))
    description = models.TextField(_('description'), null=True)
    creation_date = models.DateField(_('creation_date'), null=True)
    rating = models.FloatField(
        'Рейтинг',
        null=True,
        validators=[
            MinValueValidator(0), MaxValueValidator(100),
        ],
    )
    type = models.CharField(_('type'), max_length=10, choices=FilmWorkType.choices)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    certificate = models.CharField(_('certificate'), max_length=512, null=True)
    file_path = models.FileField(_('file'), blank=True, null=True, upload_to='movies/')

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('Filmwork')
        verbose_name_plural = _('Filmworks')

        indexes = [
            models.Index(fields=['creation_date', 'rating'], name='film_work_cr_date_rating_idx'),
        ]

    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        verbose_name = _('Filmwork Genre')
        verbose_name_plural = _('Filmwork Genres')

        constraints = [
            models.UniqueConstraint(fields=['film_work', 'genre'], name='unique_genre_film_work'),
        ]
        indexes = [
            models.Index(fields=['film_work', 'genre'], name='film_work_genre_idx'),
        ]

    def __str__(self):
        return self.film_work.title


class Gender(models.TextChoices):
    MALE = 'male', _('male')
    FEMALE = 'female', _('female')


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.TextField(_('full_name'))
    gender = models.TextField(_('gender'), choices=Gender.choices, null=True)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('Person')
        verbose_name_plural = _('Persons')

    def __str__(self):
        return self.full_name


class PersonFilmwork(UUIDMixin):

    class PersonRole(models.TextChoices):
        ACTOR = 'actor', _('actor')
        DIRECTOR = 'director', _('director')
        WRITER = 'writer', _('writer')

    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    role = models.CharField(_('role'), max_length=10, choices=PersonRole.choices, null=True)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        verbose_name = _('Filmwork Person')
        verbose_name_plural = _('Filmwork Persons')

        indexes = [
            models.Index(fields=['film_work', 'person', 'role'], name='film_work_person_idx'),
        ]

        constraints = [
            models.UniqueConstraint(fields=['film_work', 'person', 'role'], name='unique_person_film_work'),
        ]

    def __str__(self):
        return self.film_work.title
