const fs = require('fs');
const { Client } = require('pg');

// PostgreSQL connection configuration
const client = new Client({
    user: process.env.DATABASE_USER,
    host: process.env.DATABASE_HOST,
    database: process.env.DATABASE_NAME,
    password: process.env.DATABASE_PASSWORD,
    port: 5432,
});


function csvToJson(csvData) {
    const lines = csvData.split('\n');
    const headers = lines[0].trim().split(',');
    const jsonArray = [];

    for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim().split(',');
        const obj = {};

        for (let j = 0; j < headers.length; j++) {
            if (headers[j].includes('.')) {
                const [nestedField, nestedProperty] = headers[j].split('.');
                if (!obj[nestedField]) {
                    obj[nestedField] = {};
                }
                obj[nestedField][nestedProperty] = line[j];
            } else {
                obj[headers[j]] = line[j];
            }
        }

        jsonArray.push(obj);
    }

    return jsonArray;
}


client.connect()
    .then(() => {
        console.log('Connected to PostgreSQL database');

        fs.readFile('data.csv', 'utf8', async (err, csvData) => {
            if (err) {
                console.error('Error reading CSV file:', err);
                return;
            }

            const jsonData = csvToJson(csvData);

            for (const record of jsonData) {
                const { firstName, lastName } = record.name;
                const name = `${firstName} ${lastName}`;
                const { age, address, gender } = record;

                try {
                    const insertQuery = `
                        INSERT INTO public.users ("name", age, address, additional_info)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id
                    `;
                    const values = [name, age, address, { gender }];
                    const result = await client.query(insertQuery, values);
                    console.log(`Inserted row with id ${result.rows[0].id}`);
                } catch (error) {
                    console.error('Error inserting data:', error);
                }
            }

            const query = `
                SELECT
                    CASE
                        WHEN age >= 0 AND age < 18 THEN '0-17'
                        WHEN age >= 18 AND age < 30 THEN '18-29'
                        WHEN age >= 30 AND age < 40 THEN '30-39'
                        WHEN age >= 40 AND age < 50 THEN '40-49'
                        ELSE '50+'
                    END AS age_group,
                    COUNT(*) AS count
                FROM
                    public.users
                GROUP BY
                    age_group
                ORDER BY
                    age_group;
            `;

            const result = await client.query(query);

            console.log('Age Distribution:');
            console.table(result.rows);

            client.end();
        });
    })
    .catch((err) => {
        console.error('Error connecting to PostgreSQL database', err);
    });
