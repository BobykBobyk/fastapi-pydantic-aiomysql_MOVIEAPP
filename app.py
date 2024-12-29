from fastapi import FastAPI, Query, HTTPException
from typing import List
from pydantic import BaseModel
import aiomysql
from datetime import date

current_date = date.today()
current_year = current_date.year

app = FastAPI()


class Movie(BaseModel):
    id: int = Query(..., description='Enter the id for the movie')
    title: str = Query(..., description='Enter the title for the movie')
    director: str = Query('unknown', description='Enter the name of the director of the movie')
    release_year: int = Query(..., description='Enter the release year of the film')
    rating: float = Query(..., description='Enter the rating for the film')


class MoviesResponse(BaseModel):
    response: List[Movie]


async def get_db_pool():
    return await aiomysql.create_pool(
        host='localhost',
        port=3306,
        user='root',
        password='password',
        db='db',
        minsize=5,
        maxsize=10
    )


async def execute_query(query: str, params=None):
    pool = await get_db_pool()
    try:
        async with pool.acquire() as connection:
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                if query.strip().lower().startswith('select'):
                    return await cursor.fetchall()
                elif query.strip().lower().startswith(('insert', 'update', 'delete')):
                    await connection.commit()
                    return {'message': 'Query executed successfully'}
    finally:
        pool.close()
        await pool.wait_closed()


async def create_database():
    await execute_query("""
    CREATE DATABASE IF NOT EXISTS db;
    """)


async def create_table():
    await execute_query("""
    CREATE TABLE IF NOT EXISTS movies (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        director VARCHAR(255),
        release_year INT,
        rating FLOAT
    );
    """)


@app.on_event('startup')
async def startup_event():
    await create_database()
    await create_table()


@app.get('/movies')
async def get_all_movies():
    try:
        response = await execute_query("""
        SELECT * FROM movies
        """)
        movies = [Movie(**row) for row in response]
        return MoviesResponse(response=movies)

    except Exception as e:
        raise HTTPException(status_code=400, detail=f'Something went wrong, more details: {str(e)}')


@app.get('/movies/{id}')
async def get_movie_id(id_get: int):
    try:
        response = await execute_query("""
        SELECT * FROM movies WHERE id = %s
        """, (id_get,))
        movies = [Movie(**row) for row in response]
        return MoviesResponse(response=movies)

    except Exception as e:
        raise HTTPException(status_code=400, detail=f'Enter valid data, more details: {str(e)}')


@app.post('/movies')
async def add_new_movie(movie: Movie):
    if movie.release_year > current_year:
        raise HTTPException(status_code=400, detail='The film cannot be from the future')
    try:
        await execute_query("""
        INSERT INTO movies (id, title, director, release_year, rating) 
        VALUES (%s, %s, %s, %s, %s)
        """, (movie.id, movie.title, movie.director, movie.release_year, movie.rating))
        return {'message': 'The given film was added successfully'}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f'Enter valid data, more details: {str(e)}')


@app.delete('/movies/{id}')
async def delete_movie(id_get: int):
    try:
        await execute_query("""
        DELETE FROM movies WHERE id = %s
        """, (id_get,))
        return {'message': 'The movie with the given ID was deleted successfully'}

    except Exception as e:
        raise HTTPException(status_code=400, detail=f'Enter valid data, more details: {str(e)}')
