import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

// Importamos nuestros diccionarios
import translationES from './locales/es.json';
import translationEN from './locales/en.json';

// AQUÍ ESTABA EL ERROR: 
// Como los JSON ya traen la clave "translation" por dentro, 
// tenemos que apuntar directamente a ella (.translation)
const resources = {
  es: { translation: translationES.translation },
  en: { translation: translationEN.translation }
};

i18n
  .use(initReactI18next) // Conecta i18n con React
  .init({
    resources,
    lng: 'es', // <-- AQUÍ ASEGURAMOS QUE EL IDIOMA DEFAULT ES ESPAÑOL
    fallbackLng: 'en', // Idioma de reserva
    interpolation: {
      escapeValue: false // React ya protege contra XSS
    }
  });

export default i18n;