import { Link } from "react-router-dom";
import { MapPin, Calendar, Users, ExternalLink } from "lucide-react";

function formatDate(dateStr) {
  return new Date(dateStr).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export default function ClubCard({ club }) {
  const Wrapper = club.isExternal ? "a" : Link;
  const wrapperProps = club.isExternal
    ? { href: club.externalLink, target: "_blank", rel: "noopener noreferrer" }
    : { to: `/club/${club.id}` };

  return (
    <Wrapper
      {...wrapperProps}
      className="group bg-warm-50 rounded-2xl overflow-hidden border border-warm-200 hover:border-warm-300 hover:shadow-lg transition-all duration-300 flex flex-col"
    >
      <div className="h-40 overflow-hidden bg-warm-100 relative">
        <img
          src={club.thumbnail}
          alt={club.name}
          className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-500"
          loading="lazy"
        />
        <div className="absolute top-3 right-3">
          <span className="px-2.5 py-1 text-xs font-medium rounded-full bg-warm-50/90 backdrop-blur-sm text-warm-700 border border-warm-200">
            {club.genre}
          </span>
        </div>
        {club.isExternal && (
          <div className="absolute top-3 left-3">
            <ExternalLink className="w-4 h-4 text-warm-50 drop-shadow" />
          </div>
        )}
      </div>

      <div className="p-5 flex flex-col flex-1">
        <h3 className="font-serif text-lg font-semibold text-warm-900 group-hover:text-sage-700 transition-colors">
          {club.name}
        </h3>
        <p className="text-sm text-warm-500 mt-2 line-clamp-2 flex-1">
          {club.description}
        </p>

        <div className="mt-4 space-y-2 text-sm text-warm-600">
          <div className="flex items-center gap-2">
            <MapPin className="w-4 h-4 text-warm-400 shrink-0" />
            {club.location}
          </div>
          <div className="flex items-center gap-2">
            <Calendar className="w-4 h-4 text-warm-400 shrink-0" />
            {club.meetingDay} at {club.meetingTime}
          </div>
          <div className="flex items-center gap-2">
            <Users className="w-4 h-4 text-warm-400 shrink-0" />
            {club.members} members
          </div>
        </div>

        {club.currentBook && (
          <div className="mt-4 pt-4 border-t border-warm-200">
            <p className="text-xs text-warm-400 uppercase tracking-wider mb-1">
              Currently Reading
            </p>
            <p className="text-sm font-medium text-warm-700">
              {club.currentBook.title}
            </p>
          </div>
        )}
      </div>
    </Wrapper>
  );
}
