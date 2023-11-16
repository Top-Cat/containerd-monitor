FROM node:lts

RUN mkdir /app
COPY . /app
WORKDIR /app

RUN npm install

CMD ["index.mjs"]
