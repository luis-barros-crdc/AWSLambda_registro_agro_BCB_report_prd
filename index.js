const AWS = require('aws-sdk');
const sql = require('mssql');
const nodemailer = require('nodemailer');
const { S3 } = require('aws-sdk');
const { SecretsManager } = require('aws-sdk');

const getSecret = async () => {
    const secretsManager = new SecretsManager({ region: 'sa-east-1' });
    const secretName = "prd/secret_db/n_aplicacao";
    
    try {
        const data = await secretsManager.getSecretValue({ SecretId: secretName }).promise();
        if ('SecretString' in data) {
            return JSON.parse(data.SecretString);
        }
    } catch (err) {
        console.error(err);
        throw err;
    }
};

const generateCSV = (data) => {
    const headers = Object.keys(data[0]).join(';');
    const rows = data.map(row => Object.values(row).join(';')).join('\n');
    return `${headers}\n${rows}`;
};

exports.handler = async (event) => {
    try {
        const config = await getSecret();
        
        const dbConfig = {
            user: config.username,
            password: config.password,
            server: config.host,
            database: config.dbInstanceIdentifier,
            options: {
                encrypt: true,
                enableArithAbort: true,
                trustServerCertificate: true,
            },
        };

        await sql.connect(dbConfig);

        const inputData = event?.dataReferencia;
        const ref = inputData ? new Date(`${inputData}T00:00:00Z`) : new Date();
        
        const pad = n => n.toString().padStart(2, '0');
        const YY = ref.getUTCFullYear();
        const MM = pad(ref.getUTCMonth() + 1);
        const DD = pad(ref.getUTCDate());
        
        const dataReferenciaSQL = `${YY}-${MM}-${DD} 23:59:59.000`;
        const dataReferenciaDDMMYYYY = `${DD}${MM}${YY}`;

        // Consulta BCB A1 simplificada
        const query1 = `SELECT 
            a.tipo_ativo,
            a.numero_ativo,
            e.cpf_cnpj,
            e.nome,
            FORMAT(a.data_emissao, 'ddMMyyyy') AS data_emissao,
            FORMAT(a.data_vencimento, 'ddMMyyyy') AS data_vencimento,
            a.valor,
            p.tipo_produto
        FROM REGISTRY_OTHER_ASSET.ativo a
        LEFT JOIN REGISTRY_OTHER_ASSET.emitente_principalcpr e ON a.ativo_cpr_emitente_principal_id = e.id
        LEFT JOIN REGISTRY_OTHER_ASSET.produto p ON a.ativo_cpr_produto_id = p.id
        WHERE a.data_criacao <= '${dataReferenciaSQL}'
            AND a.numero_ativo NOT LIKE '%tst%'
            AND a.numero_ativo NOT LIKE '%teste%'`;

        // Consulta BCB A2 simplificada
        const query2 = `SELECT 
            a.tipo_ativo,
            a.numero_ativo,
            e.cpf_cnpj,
            e.nome,
            c.nome_razao_social,
            FORMAT(a.data_emissao, 'ddMMyyyy') AS data_emissao,
            a.valor
        FROM REGISTRY_OTHER_ASSET.ativo a
        LEFT JOIN REGISTRY_OTHER_ASSET.emitente_principalcpr e ON a.ativo_cpr_emitente_principal_id = e.id
        LEFT JOIN REGISTRY_OTHER_ASSET.credor c ON a.ativo_cpr_credor_id = c.id
        WHERE a.data_criacao <= '${dataReferenciaSQL}'
            AND a.numero_ativo NOT LIKE '%tst%'`;

        const result1 = await sql.query(query1);
        const result2 = await sql.query(query2);
        
        const csvData1 = generateCSV(result1.recordset);
        const csvData2 = generateCSV(result2.recordset);
        
        const s3 = new S3();
        const reportFilename1 = `BCB_A1_${dataReferenciaDDMMYYYY}.csv`;
        const reportFilename2 = `BCB_A2_${dataReferenciaDDMMYYYY}.csv`;
        const s3Bucket = "crdc-prd-registro-report";

        await s3.putObject({
            Bucket: s3Bucket,
            Key: `reports/${reportFilename1}`,
            Body: csvData1,
            ContentType: 'text/csv'
        }).promise();

        await s3.putObject({
            Bucket: s3Bucket,
            Key: `reports/${reportFilename2}`,
            Body: csvData2,
            ContentType: 'text/csv'
        }).promise();

        const transporter = nodemailer.createTransporter({
            SES: new AWS.SES({ region: 'sa-east-1' })
        });

        const mailOptions = {
            from: 'report_noreply@crdc.com.br',
            to: ['compliance@crdc.com.br', 'luis.barros@crdc.com.br'],
            subject: `Relatórios CSV BCB A1 e A2 ${dataReferenciaDDMMYYYY}`,
            text: `Os relatórios estão disponíveis no S3 e em anexo.`,
            attachments: [
                { filename: reportFilename1, content: csvData1 },
                { filename: reportFilename2, content: csvData2 }
            ]
        };

        await transporter.sendMail(mailOptions);

        return {
            statusCode: 200,
            body: 'Relatórios gerados e enviados com sucesso!'
        };
    } catch (error) {
        console.error(error);
        return {
            statusCode: 500,
            body: JSON.stringify(error)
        };
    }
};